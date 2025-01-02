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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Thread reading JsonEvents from S3 into a buffer, controlling access to the buffer with a semaphore.
 */
public class JsonEventBufferedReader extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(JsonEventBufferedReader.class);

    private boolean hasNext = true;

    private final JsonEventS3Iterator sourceEventIterator;
    private final int bufferSize;
    private final Semaphore semaphore;
    private final PriorityBlockingQueue<JsonEvent> buffer;

    public JsonEventBufferedReader(JsonEventS3Iterator sourceEventIterator, int bufferSize) {
        this.sourceEventIterator = sourceEventIterator;
        this.bufferSize = bufferSize;
        this.semaphore = new Semaphore(bufferSize);
        this.buffer = new PriorityBlockingQueue<>(bufferSize, JsonEvent.timestampComparator);
    }

    @Override
    public void run() {
        try {
            LOG.debug("Start reading from source iterator");
            while (!Thread.currentThread().isInterrupted()) {
                // Iterates events from the source iterator until there is any available
                if (sourceEventIterator.hasNext()) {
                    semaphore.acquire();

                    buffer.add(sourceEventIterator.next());
                } else {
                    hasNext = false;
                    LOG.debug("No more events from the source iterator");
                    Thread.currentThread().interrupt();
                }
            }
        } catch (InterruptedException e) {
            LOG.debug("Buffered reader thread interrupted");
            // Allow thread to exit
        }
    }

    public boolean hasNext() {
        return hasNext || !buffer.isEmpty();
    }

    public JsonEvent take() throws InterruptedException {
        semaphore.release();

        return buffer.take();
    }

    public JsonEvent peek() {
        return buffer.peek();
    }

    public int bufferCapacity() {
        return buffer.size();
    }


    /**
     * Wait until the buffer is filled to its capacity
     */
    public void fill() throws InterruptedException {
        LOG.debug("Filling up {} records", bufferSize - buffer.size());
        while (buffer.size() < bufferSize) {
            Thread.sleep(500);
        }
    }

}
