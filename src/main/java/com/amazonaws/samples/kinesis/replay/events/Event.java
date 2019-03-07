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

package com.amazonaws.samples.kinesis.replay.events;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;
import software.amazon.awssdk.core.SdkBytes;


public class Event {
  public static final String TYPE_FIELD = "type";

  public final String payload;

  public Event(String payload) {
    if (!payload.endsWith("\n")) {
      //append a newline to output to make it easier digestible by firehose and athena
      this.payload = payload + "\n";
    } else {
      this.payload = payload;
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
    return SdkBytes.fromString(payload, Charset.forName("UTF-8"));
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(payload.getBytes(Charset.forName("UTF-8")));
  }
}
