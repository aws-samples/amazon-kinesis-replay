/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.samples.kinesis.replay2.utils;

import com.amazonaws.samples.kinesis.replay2.events.JsonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

public class KinesisBufferedProducer extends Thread implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisBufferedProducer.class);

    private static final int MAX_RECORD_PER_REQUEST = 500; // TODO make configurable
    private static final long INITIAL_RETRY_DELAY_MS = 100; // TODO make configurable
    private static final int MAX_RETRIES = 100; // TODO make configurable

    private static final long WAIT_IF_NO_BUFFERED_EVENTS_MS = 500;

    private final String streamArn;
    private final ArrayBlockingQueue<JsonEvent> buffer;
    private final KinesisClient kinesis;

    private final List<JsonEvent> batch = new ArrayList<>(MAX_RECORD_PER_REQUEST);

    public KinesisBufferedProducer(String streamArn, int bufferSize) {
        this.streamArn = streamArn;
        buffer = new ArrayBlockingQueue<>(bufferSize);
        kinesis = KinesisClient.builder().build();
    }

    /**
     * Queue an event to be sent to Kinesis, blocking if the buffer is full
     */
    public void queue(JsonEvent event) throws InterruptedException {
        buffer.put(event);
    }

    @Override
    public void run() {
        try {
            LOG.debug("Start producing events to Kinesis: {}",streamArn);
            while (!Thread.currentThread().isInterrupted()) {
                sendABatch();
            }
        } catch (InterruptedException e) {
            // Let the thread exit
            LOG.debug("Producer thread interrupted");
        }
    }

    @Override
    public void close() {
        if(kinesis != null) {
            kinesis.close();
        }
    }

    private void sendABatch() throws InterruptedException {
        // Fetch up to records-per-request events from the buffer
        batch.clear();
        int batchSize = buffer.drainTo(batch, MAX_RECORD_PER_REQUEST);
        if (batchSize > 0) {
            LOG.trace("Sending {} events with a PutRecords request", batchSize);

            // Prepare the PutRecords request
            List<PutRecordsRequestEntry> requestEntries = new ArrayList<>(batchSize);
            for (JsonEvent event : batch) {
                requestEntries.add(PutRecordsRequestEntry.builder()
                        .data(event.toSdkBytes())
                        .partitionKey(randomPartitionKey())
                        .build());
            }
            PutRecordsRequest request = PutRecordsRequest.builder()
                    .streamARN(streamArn)
                    .records(requestEntries)
                    .build();


            // Selectively resend failed records
            int failedRecordCount;
            PutRecordsResponse response;
            int attempts = 0;
            while ((failedRecordCount = (response = attemptPutRecords(request, attempts)).failedRecordCount()) > 0) {
                attempts++;
                LOG.debug("Retrying {} failed records reported by PutRecords", failedRecordCount);
                List<PutRecordsRequestEntry> failedRecords = new ArrayList<>(failedRecordCount);
                List<PutRecordsResultEntry> resultEntries = response.records();
                // Requeue failed records
                for (int i = 0; i < resultEntries.size(); i++) {
                    if (resultEntries.get(i).errorCode() != null) {
                        failedRecords.add(requestEntries.get(i));
                    }
                }
                // Prepare new request with failed
                request = PutRecordsRequest.builder()
                        .streamARN(streamArn)
                        .records(failedRecords)
                        .build();
            }
            LOG.trace("PutRecords successfully completed");

        } else {
            LOG.debug("No event waiting in the buffer. Producer thread will sleep for {} ms", WAIT_IF_NO_BUFFERED_EVENTS_MS);
            Thread.sleep(WAIT_IF_NO_BUFFERED_EVENTS_MS);
        }
    }

    private String randomPartitionKey() {
        return UUID.randomUUID().toString();
    }


    private PutRecordsResponse attemptPutRecords(PutRecordsRequest request, int attempt) {
        if ( attempt == 0 ) {
            // First attempt: no delay
            return kinesis.putRecords(request);
        } else if (attempt < MAX_RETRIES) {
            long backoffTime = (long) Math.pow(2, attempt) * INITIAL_RETRY_DELAY_MS;
            LOG.trace("Retrying PutRecords due to provisioned throughput exception. Attempt # {}, Backoff {} ms", attempt, backoffTime);
        } else {
            throw new RuntimeException("Failed to execute PutRecords after " + attempt + " retries");
        }

        return kinesis.putRecords(request);
    }
}
