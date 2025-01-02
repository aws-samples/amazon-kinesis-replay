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

package com.amazonaws.samples.kinesis.replay2.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;

/**
 * Event wrapping a JSON content.
 * The parser extracts event timestamp and ingestion time from
 * a given JSON attribute.
 *
 * Ingestion time is simulated, reading events at the rate based on event time multiplied by a speedup factor.
 */
public class JsonEvent {
    public final String payload;
    public final Instant timestamp;
    public final Instant ingestionTime;

    public JsonEvent(String payload, Instant timestamp, Instant ingestionTime) {
        if (!payload.endsWith("\n")) {
            //append a newline to output to make it easier digestible by firehose and athena
            this.payload = payload + "\n";
        } else {
            this.payload = payload;
        }

        this.timestamp = timestamp;
        this.ingestionTime = ingestionTime;
    }


    public static final Comparator<JsonEvent> timestampComparator =
            (JsonEvent o1, JsonEvent o2) -> o1.timestamp.compareTo(o2.timestamp);

    public static final Comparator<JsonEvent> ingestionTimeComparator =
            (JsonEvent o1, JsonEvent o2) -> o1.ingestionTime.compareTo(o2.ingestionTime);

    /**
     * Event parser.
     * A single instance must be used for all events of a stream.
     * Not thread-safe.
     */
    public static class Parser {
        private final ObjectMapper mapper = new ObjectMapper();

        private Instant ingestionStartTime = Instant.now(); // Ingestion time is in world-clock domain
        private Instant firstEventTime;

        private final float speedupFactor;
        private final String timestampAttributeName;

        public Parser(float speedupFactor, String timestampAttributeName) {
            this.speedupFactor = speedupFactor;
            this.timestampAttributeName = timestampAttributeName;
        }

        public JsonEvent parse(String payload) {
            JsonNode json;
            try {
                json = mapper.readTree(payload);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // Parse event time from the JSON payload
            Instant eventTime = Instant.parse(json.get(timestampAttributeName).asText());
            if (firstEventTime == null) {
                firstEventTime = eventTime;
            }

            // Ingestion time is simulated, based on the event time
            long deltaToFirstEventTime = Math.round(Duration.between(firstEventTime, eventTime).toMillis() / speedupFactor);
            Instant ingestionTime = ingestionStartTime.plusMillis(deltaToFirstEventTime);

            return new JsonEvent(payload, eventTime, ingestionTime);
        }

        public void reset() {
            firstEventTime = null;
            ingestionStartTime = Instant.now();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload);
    }

    @Override
    public String toString() {
        return payload;
    }

    public SdkBytes toSdkBytes() {
        return SdkBytes.fromString(payload, StandardCharsets.UTF_8);
    }

    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8));
    }
}
