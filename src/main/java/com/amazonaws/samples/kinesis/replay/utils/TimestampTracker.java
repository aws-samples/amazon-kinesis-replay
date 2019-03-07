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
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampTracker {
  private final Map<String, Instant> largestPossibleWatermark = new HashMap<>();
  private final PriorityBlockingQueue<JsonEvent> inflightEvents = new PriorityBlockingQueue<>(500, JsonEvent.timestampComparator);

  private static final Logger LOG = LoggerFactory.getLogger(TimestampTracker.class);

  /** Track the timestamp of the event for determining watermark values until it has been sent or dropped. */
  public void trackTimestamp(ListenableFuture<UserRecordResult> f, JsonEvent event) {
    Futures.addCallback(f, new RemoveTimestampCallback(event));
  }

  public Instant getWatermark(String shardId) {
    if (largestPossibleWatermark.containsKey(shardId)) {
      return largestPossibleWatermark.get(shardId);
    } else {
      LOG.debug("no watermark information available for shard {}, defaulting to Instant.EPOCH");

      return Instant.EPOCH;
    }
  }

  public Instant getMinWatermark(List<String> shardIds) {
    try {
      return largestPossibleWatermark
          .entrySet()
          .parallelStream()
          .filter(entry -> shardIds.contains(entry.getKey()))
          .map(entry -> entry.getValue())
          .min(Comparator.naturalOrder())
          .get();
    } catch (NoSuchElementException e) {
      //if there is no information on timestamps yet, just return smallest possible value

      return Instant.EPOCH;
    }
  }

  /**
   * Helper class that adds and event (and it's timestamp) to a priority queue
   * and remove it when it has eventually been sent to the Kinesis stream or was dropped by the KCL.
   */
  private class RemoveTimestampCallback implements FutureCallback<UserRecordResult> {
    private final JsonEvent event;

    RemoveTimestampCallback(JsonEvent event) {
      this.event = event;

      inflightEvents.add(event);
    }

    private void removeEvent() {
      boolean queueChanged = inflightEvents.remove(event);

      if (!queueChanged) {
        LOG.warn("couldn't find event in inflights queue, it was already removed: {}", event);
      }

    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("failed to send event {}", event);

      removeEvent();
    }

    @Override
    public void onSuccess(UserRecordResult result) {
      removeEvent();

      JsonEvent oldestEventInQueue = inflightEvents.peek();

      //determine the larges possible watermark value, this assumes that events are read and sent in order
      if (oldestEventInQueue == null || event.timestamp.isAfter(oldestEventInQueue.timestamp)) {
        return;
      }

      largestPossibleWatermark.put(result.getShardId(), event.timestamp);
    }
  }
}
