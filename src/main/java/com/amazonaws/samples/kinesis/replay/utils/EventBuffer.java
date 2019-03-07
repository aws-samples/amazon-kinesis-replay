/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazonaws.samples.kinesis.replay.utils;

import com.amazonaws.samples.kinesis.replay.events.JsonEvent;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;

public class EventBuffer extends Thread {
  private static final int MAX_QUEUE_LENGTH = 100_000;

  private boolean hasNext = true;

  private EventReader reader;
  private final Semaphore semaphore = new Semaphore(MAX_QUEUE_LENGTH);
  private final PriorityBlockingQueue<JsonEvent> eventPool = new PriorityBlockingQueue<>(MAX_QUEUE_LENGTH, JsonEvent.timestampComparator);

  public EventBuffer(EventReader reader) {
    this.reader = reader;
  }

  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        if (reader.hasNext()) {
          semaphore.acquire();

          eventPool.add(reader.next());
        } else {
          hasNext = false;

          Thread.currentThread().interrupt();
        }
      }
    } catch (InterruptedException e) {
      //allow thread to exit
    }
  }

  public boolean hasNext() {
    return hasNext || !eventPool.isEmpty();
  }

  public JsonEvent take() throws InterruptedException {
    semaphore.release();

    return eventPool.take();
  }

  public JsonEvent peek() {
    return eventPool.peek();
  }

  public double getFillLevel() {
    return (double) eventPool.size()/MAX_QUEUE_LENGTH;
  }
}
