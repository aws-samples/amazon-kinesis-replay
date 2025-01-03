package com.amazonaws.samples.kinesis.replay2.utils;

import com.amazonaws.samples.kinesis.replay2.events.JsonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KinesisProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducer.class);

    private static final long WAIT_IF_NO_BUFFERED_EVENTS_MS = 500;
    private static final int DEFAULT_MAX_RECORD_PER_REQUEST = 500;
    private static final long INITIAL_RETRY_DELAY_MS = 100;
    private static final int DEFAULT_MAX_RETRIES = 100;
    private static final long MAX_RETRY_DELAY_MS = 2000;

    private final int bufferCapacity;
    private final int maxEntriesPerRequest;
    private final int maxRetryCount;

    private final ArrayBlockingQueue<JsonEvent> buffer;
    private final List<Sender> senders = new ArrayList<>();


    // Stats
    private final AtomicLong statsTotalRequestCount = new AtomicLong();
    private final AtomicLong statsTotalRetryCount = new AtomicLong();

    public KinesisProducer(String streamArn, int bufferCapacity, int senderThreads, int maxEntriesPerRequest, int maxRetryCount) {
        validateArn(streamArn);

        this.bufferCapacity = bufferCapacity;
        this.maxEntriesPerRequest = maxEntriesPerRequest;
        this.maxRetryCount = maxRetryCount;
        LOG.info("Starting Kinesis producer to {} ({} sender threads, {} max entries per PutRecords request, {} max retry count)", streamArn, senderThreads, maxEntriesPerRequest, maxRetryCount);

        buffer = new ArrayBlockingQueue<>(bufferCapacity);
        for (int i = 0; i < senderThreads; i++) {
            Sender sender = new Sender(streamArn, i);
            senders.add(sender);
        }
    }

    public KinesisProducer(String streamArn, int bufferCapacity, int senderThreads) {
        this(streamArn, bufferCapacity, senderThreads, DEFAULT_MAX_RECORD_PER_REQUEST, DEFAULT_MAX_RETRIES);
    }

    private static void validateArn(String arn) {
        String arnRegex = "^arn:aws:kinesis:[a-z0-9-]+:\\d{12}:stream/[a-zA-Z0-9_.-]+$";
        Pattern pattern = Pattern.compile(arnRegex);
        if (!pattern.matcher(arn).matches()) {
            throw new IllegalArgumentException("Invalid Stream ARN: " + arn);
        }
    }

        private static Region extractRegion (String streamArn){
            String region = streamArn.split(":")[3];
            return Region.of(region);
        }

        public void startSenders () {
            for (Sender sender : senders) {
                LOG.debug("Starting sender {}", sender.getName());
                sender.start();
            }
        }

        public void stopSenders () {
            for (Sender sender : senders) {
                LOG.debug("Stopping sender {}", sender.getName());
                sender.interrupt();
            }
        }

        public void queue (JsonEvent event) throws InterruptedException {
            buffer.put(event);
        }


        private class Sender extends Thread {
            private final KinesisClient kinesis;
            private final String streamArn;
            private final List<JsonEvent> batch = new ArrayList<>(maxEntriesPerRequest);

            Sender(String streamArn, int senderIndex) {
                super("KinesisSender-" + senderIndex);
                kinesis = KinesisClient.builder().region(extractRegion(streamArn)).build();
                this.streamArn = streamArn;
            }

            @Override
            public void run() {
                LOG.debug("Sender starts producing events to {}", streamArn);
                try {
                    while (!this.isInterrupted()) {
                        batch.clear();
                        int batchSize;
                        // Try to get a batch from the buffer
                        if ((batchSize = buffer.drainTo(batch, maxEntriesPerRequest)) > 0) {
                            LOG.trace("Sending {} events with a PutRecords request", batchSize);
                            sendBatch();
                        } else {
                            // If the buffer is empty, wait for a short time before next poll
                            LOG.trace("No event waiting in the buffer. Sender thread will sleep for {} ms", WAIT_IF_NO_BUFFERED_EVENTS_MS);
                            Thread.sleep(WAIT_IF_NO_BUFFERED_EVENTS_MS);
                        }
                    }
                } catch (InterruptedException ie) {
                    LOG.debug("Producer thread interrupted");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.error("Error in sender thread", e);
                    System.exit(1);
                } finally {
                    kinesis.close();
                }
            }

            private void sendBatch() {
                // Prepare the PutRecords request
                List<PutRecordsRequestEntry> requestEntries = requestEntries(batch);
                LOG.trace("Sending a batch of {} events", batch.size());
                attemptPutRecords(requestEntries, 0);
            }

            private void attemptPutRecords(List<PutRecordsRequestEntry> requestEntries, int attempt) {
                if (attempt > maxRetryCount) {
                    LOG.warn("Max retries exceeded after {} attempts", attempt);
                    throw new RuntimeException("Max retries exceeded after " + attempt + " attempts");
                } else if (attempt > 0) {
                    statsTotalRetryCount.incrementAndGet();

                    // Exponential backoff on retries
                    long backoffTime = Math.min(MAX_RETRY_DELAY_MS, (long) Math.pow(2, attempt) * INITIAL_RETRY_DELAY_MS);
                    LOG.trace("Retrying PutRecords due to provisioned throughput exception. Attempt # {}, Backoff {} ms", attempt, backoffTime);
                    try {
                        Thread.sleep(backoffTime);
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while sending records", e);
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                // Execute PutRecords
                PutRecordsResponse response = kinesis.putRecords(prepareRequest(requestEntries));
                statsTotalRequestCount.incrementAndGet();

                // If the response contains any errors, requeue them recursively
                if (response.failedRecordCount() > 0) {
                    List<PutRecordsRequestEntry> failedEntries = requeueFailedEntries(response.records(), requestEntries);
                    attemptPutRecords(failedEntries, attempt + 1);
                }

                LOG.trace("Successfully sent {} events", requestEntries.size());
            }

            private PutRecordsRequest prepareRequest(List<PutRecordsRequestEntry> requestEntries) {
                return PutRecordsRequest.builder()
                        .streamARN(streamArn)
                        .records(requestEntries)
                        .build();
            }

            private List<PutRecordsRequestEntry> requestEntries(List<JsonEvent> events) {
                return events.stream()
                        .map(event -> PutRecordsRequestEntry.builder()
                                .data(event.toSdkBytes())
                                .partitionKey(randomPartitionKey())
                                .build())
                        .collect(Collectors.toList());
            }

            private List<PutRecordsRequestEntry> requeueFailedEntries(List<PutRecordsResultEntry> resultEntries, List<PutRecordsRequestEntry> originalRequestEntries) {
                List<PutRecordsRequestEntry> failedRecords = new ArrayList<>();
                for (int i = 0; i < resultEntries.size(); i++) {
                    if (resultEntries.get(i).errorCode() != null) {
                        failedRecords.add(originalRequestEntries.get(i));
                    }
                }
                return failedRecords;
            }

            private String randomPartitionKey() {
                return UUID.randomUUID().toString();
            }
        }

        ///  Expose stats

        public int bufferedEventCount () {
            return buffer.size();
        }

        public int bufferCapacity () {
            return bufferCapacity;
        }

        public long totalRequestCount () {
            return statsTotalRequestCount.get();
        }

        public long totalRetryCount () {
            return statsTotalRetryCount.get();
        }

    }
