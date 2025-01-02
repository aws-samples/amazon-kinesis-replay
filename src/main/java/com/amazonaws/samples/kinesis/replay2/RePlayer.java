package com.amazonaws.samples.kinesis.replay2;

import com.amazonaws.samples.kinesis.replay2.events.JsonEvent;
import com.amazonaws.samples.kinesis.replay2.utils.JsonEventBufferedReader;
import com.amazonaws.samples.kinesis.replay2.utils.JsonEventS3Iterator;
import com.amazonaws.samples.kinesis.replay2.utils.KinesisBufferedProducer;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;

public class RePlayer {
    private static final Logger LOG = LoggerFactory.getLogger(RePlayer.class);

    private static final String DEFAULT_BUCKET_REGION = "us-east-1";
    private static final String DEFAULT_BUCKET_NAME = "aws-bigdata-blog";
    private static final String DEFAULT_OBJECT_PREFIX = "artifacts/kinesis-analytics-taxi-consumer/taxi-trips.json.lz4/";
    private static final float DEFAULT_SPEEDUP_FACTOR = 6480;
    private static final long DEFAULT_STATISTICS_FREQUENCY_MILLIS = 20000;
    private static final String DEFAULT_TIMESTAMP_ATTRIBUTE_NAME = "dropoff_datetime";
    private static final int DEFAULT_READER_BUFFER_SIZE = 100_000;
    private static final int DEFAULT_PRODUCER_BUFFER_SIZE = 1000;

    private final JsonEventBufferedReader bufferedReader;
    private final KinesisBufferedProducer kinesisProducer;
    private final String s3SourcePrefix; // For logging only
    private final String kinesisDestinationStreamArn; // For logging only
    private final long statisticsFrequencyMillis;

    public RePlayer(String bucketRegion, String bucketName, String objectPrefix, String streamArn, String timestampAttributeName, float speedupFactor, long statisticsFrequencyMillis, Instant seekToEpoch, int readerBufferSize, int producerBufferSize) {
        s3SourcePrefix = String.format("s3://%s/%s", bucketName, objectPrefix);
        kinesisDestinationStreamArn = streamArn;
        LOG.info("Replay events from {} into Kinesis stream {}", s3SourcePrefix, kinesisDestinationStreamArn);

        this.statisticsFrequencyMillis = statisticsFrequencyMillis;

        // Initialize S3 reader
        S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();
        JsonEventS3Iterator sourceIterator = new JsonEventS3Iterator(s3, bucketName, objectPrefix, speedupFactor, timestampAttributeName);

        // Seek start position, if required
        if (seekToEpoch != null) {
            sourceIterator.seek(seekToEpoch);
        }

        // Start S3 reader thread
        bufferedReader = new JsonEventBufferedReader(sourceIterator, readerBufferSize);
        bufferedReader.start();


        // Initialize Kinesis Producer
        kinesisProducer = new KinesisBufferedProducer(streamArn, producerBufferSize);
        kinesisProducer.start();
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options()
                .addRequiredOption("s", "streamArn", true, "the ARN of the kinesis stream the events are sent to")
                .addOption("bucketRegion", true, "the region of the S3 bucket")
                .addOption("bucketName", true, "the bucket containing the raw event data")
                .addOption("objectPrefix", true, "the prefix of the objects containing the raw event data")
                .addOption("speedup", true, "the speedup factor for replaying events into the kinesis stream")
                .addOption("timestampAttributeName", true, "the name of the attribute that contains the timestamp according to which events ingested into the stream")
                .addOption("seek", true, "start replaying events at given timestamp")
                .addOption("statisticsFrequency", true, "print statistics every statisticFrequency ms")
                .addOption("readerBufferSize", true, "size of the buffer that holds events read from S3")
                .addOption("kinesisProducerBuffer", true, "size of the buffer of the Kinesis producer")
                .addOption("help", "print this help message");

        CommandLine line = new DefaultParser().parse(options, args);
        if (line.hasOption("help")) {
            new HelpFormatter().printHelp(MethodHandles.lookup().lookupClass().getName(), options);
        } else {
            String streamArn = line.getOptionValue("streamArn");
            String bucketRegion = line.getOptionValue("bucketRegion", DEFAULT_BUCKET_REGION);
            String bucketName = line.getOptionValue("bucketName", DEFAULT_BUCKET_NAME);
            String objectPrefix = line.getOptionValue("objectPrefix", DEFAULT_OBJECT_PREFIX);
            float speedupFactor = Float.parseFloat(line.getOptionValue("speedup", String.valueOf(DEFAULT_SPEEDUP_FACTOR)));
            String timestampAttributeName = line.getOptionValue("timestampAttributeName", DEFAULT_TIMESTAMP_ATTRIBUTE_NAME);
            Instant seekToEpoch = line.hasOption("seek") ? Instant.parse(line.getOptionValue("seek")) : null;
            long statisticsFrequencyMillis = Long.parseLong(line.getOptionValue("statisticsFrequency", String.valueOf(DEFAULT_STATISTICS_FREQUENCY_MILLIS)));
            int bufferSize = Integer.parseInt(line.getOptionValue("bufferSize", String.valueOf(DEFAULT_READER_BUFFER_SIZE)));
            int kinesisProducerBuffer = Integer.parseInt(line.getOptionValue("kinesisProducerBuffer", String.valueOf(DEFAULT_PRODUCER_BUFFER_SIZE)));

            LOG.debug("RePlayer configuration:" +
                            "\n\tDestination stream ARN: {}" +
                            "\n\tS3 source (region: {}): s3://{}/{}" +
                            "\n\tSpeedup factor: {}" +
                            "\n\tTimestamp attribute name: {}" +
                            "\n\tStatistics frequency: {} ms" +
                            "\n\tBuffer size: {}" +
                            "\n\tKinesis producer buffer: {}",
                    streamArn, bucketRegion, bucketName, objectPrefix, speedupFactor, timestampAttributeName, statisticsFrequencyMillis, bufferSize, kinesisProducerBuffer);
            if (seekToEpoch != null) {
                LOG.debug("\n\tSeek first event @ {}", seekToEpoch);
            }

            RePlayer rePlayer = new RePlayer(bucketRegion, bucketName, objectPrefix, streamArn, timestampAttributeName, speedupFactor, statisticsFrequencyMillis, seekToEpoch, bufferSize, kinesisProducerBuffer);
            rePlayer.replay();
        }
    }

    private void replay() {
        long statisticsTotalEventCount = 0;
        long statisticsBatchEventCount = 0;
        long statisticsLastOutputTimeslot = 0;

        try {
            LOG.info("Populate reader buffer");
            bufferedReader.fill();

            if (bufferedReader.peek() == null) {
                LOG.error("Couldn't find any events to replay in {}", s3SourcePrefix);
                return;
            }

            LOG.info("Start replaying events into stream {}", kinesisDestinationStreamArn);
            final long ingestionStartTime = System.currentTimeMillis();

            // Read until there are available events in the source
            do {
                JsonEvent event = bufferedReader.take();

                // If it is reading too fast, pause the rePlayer thread
                Duration replayTimeGap = Duration.between(event.ingestionTime, Instant.now());
                if (replayTimeGap.isNegative()) {
                    LOG.trace("Reading too fast from the source. Sleep for {} ms", replayTimeGap);
                    Thread.sleep(-replayTimeGap.toMillis());
                }

                // Queue event to Kinesis (block if producer buffer is full)
                kinesisProducer.queue(event);

                statisticsBatchEventCount++;
                statisticsTotalEventCount++;

                // Periodically output statistics
                if ((System.currentTimeMillis() - ingestionStartTime) / statisticsFrequencyMillis != statisticsLastOutputTimeslot) {
                    double statisticsBatchEventRate = Math.round(1000.0 * statisticsBatchEventCount / statisticsFrequencyMillis);

                    if (LOG.isDebugEnabled() || LOG.isTraceEnabled()) {
                        LOG.debug("All events with dropoff time until {} have been sent ({} events, {} events/sec, {} replay lag, {} buffer capacity)",
                                event.timestamp, statisticsTotalEventCount, statisticsBatchEventRate, Duration.ofSeconds(replayTimeGap.getSeconds()), bufferedReader.bufferCapacity());
                    } else {
                        LOG.info("All events with dropoff time until {} have been sent ({} events, {} events/sec, {} replay lag)",
                                event.timestamp, statisticsTotalEventCount, statisticsBatchEventRate, Duration.ofSeconds(replayTimeGap.getSeconds()));
                    }

                    statisticsBatchEventCount = 0;
                    statisticsLastOutputTimeslot = (System.currentTimeMillis() - ingestionStartTime) / statisticsFrequencyMillis;
                }

            } while (bufferedReader.hasNext());

            LOG.info("All events have been sent to Kinesis");
        } catch (InterruptedException e) {
            // Exit gracefully
        } finally {
            LOG.info("Replay stopped");

            // Stop reader
            bufferedReader.interrupt();
            // Stop producer
            kinesisProducer.interrupt();
        }
    }
}
