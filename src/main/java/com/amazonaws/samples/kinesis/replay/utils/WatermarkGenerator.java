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
import com.amazonaws.samples.kinesis.replay.events.WatermarkEvent;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;


public class WatermarkGenerator extends Thread {
  private final String streamName;
  private final KinesisClient kinesisClient;

  private long lastShardRefreshTime = 0;
  private long lastWatermarkSentTime = 0;
  private long sentEventCount = 0;

  private List<Shard> shards = new ArrayList<>();
  private final TimestampTracker timestampTracker = new TimestampTracker();

  /** Sent a watermark every WATERMARK_MILLIS ms or WATERMARK_EVENT_COUNT events, whatever comes first. */
  private static final long WATERMARK_MILLIS = 1_000;
  private static final long WATERMARK_EVENT_COUNT = 20_000;

  private static final long SHARD_REFRESH_MILLIES = 10_000;
  private static final long SLEEP_MILLIES = 100;

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkGenerator.class);


  public WatermarkGenerator(Region streamRegion, String streamName) {
    this.streamName = streamName;
    this.kinesisClient = KinesisClient.builder().region(streamRegion).build();
  }

  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        //refresh the list of available shards, if current state is too old
        if (System.currentTimeMillis() - lastShardRefreshTime >= SHARD_REFRESH_MILLIES) {
          try {
            refreshShards();

            lastShardRefreshTime = System.currentTimeMillis();
          } catch (LimitExceededException | ResourceInUseException e) {
            //if the request is throttled, just wait for the next invocation and use cached shard description in the meantime
            LOG.debug("skipping shard refresh due to limit exceeded/resource in use exception");
          }
        }

        //sent watermark if WATERMARK_MILLIS have passed or more than WATERMARK_EVENT_COUNT events have been sent
        if (System.currentTimeMillis() - lastWatermarkSentTime >= WATERMARK_MILLIS || sentEventCount >= WATERMARK_EVENT_COUNT) {
          sentWatermarkToShards();

          sentEventCount = 0;
          lastWatermarkSentTime = System.currentTimeMillis();
        }

        Thread.sleep(SLEEP_MILLIES);
      }
    } catch (InterruptedException | AbortedException e ) {
      //allow thread to exit
    }
  }

  public void trackTimestamp(ListenableFuture<UserRecordResult> f, JsonEvent event) {
    timestampTracker.trackTimestamp(f, event);
  }

  public WatermarkEvent createWatermark(String shardId) {
    return new WatermarkEvent(timestampTracker.getWatermark(shardId));
  }

  public Instant getMinWatermark() {
    List<String> shardIds = shards
        .stream()
        .map(shard -> shard.shardId())
        .collect(Collectors.toList());

    return timestampTracker.getMinWatermark(shardIds);
  }

  private void sentWatermarkToShards() {
    //send a watermark to every shard of the Kinesis stream
    try {
      shards
          .parallelStream()
          .map(shard -> PutRecordRequest
              .builder()
              .streamName(streamName)
              .data(createWatermark(shard.shardId()).toSdkBytes())
              .partitionKey("23")
              .explicitHashKey(shard.hashKeyRange().startingHashKey())
              .build())
          .map(request -> {
              LOG.trace("sending watermark '{}' with explicit hash key {}", request.data().toString(), request.explicitHashKey());

              return kinesisClient.putRecord(request);
          })
          .forEach(putRecordResult -> LOG.trace("sent watermark to shard {}", putRecordResult.shardId()));

      LOG.debug("send watermarks to shards");
    } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
      //if any request is throttled, just skip this watermark and wait for the next iteration to submit another one
      LOG.warn("skipping watermark due to limit/throughput exceeded exception");
    }
  }

  private void refreshShards() {
    String nextToken = "";
    List<Shard> openShards = new ArrayList<>();

    do {
      final ListShardsRequest.Builder request = ListShardsRequest.builder();
      if (StringUtils.isEmpty(nextToken)) {
        request.streamName(streamName);
      } else {
        request.nextToken(nextToken);
      }

      ListShardsResponse result = kinesisClient.listShards(request.build());

      //find open shards and add them to openShards
      result.shards()
          .stream()
          .filter(shard -> shard.sequenceNumberRange().endingSequenceNumber()==null)
          .forEach(openShards::add);

      nextToken = result.nextToken();
    } while (!StringUtils.isEmpty(nextToken));

    this.shards = openShards;
  }
}