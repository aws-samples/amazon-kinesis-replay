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
import java.io.*;
import java.time.Instant;
import java.util.Iterator;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;


public class EventReader implements Iterator<JsonEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(EventReader.class);

  private final String bucketName;
  private final S3Client s3;
  private final Iterator<S3Object> s3Objects;

  private final JsonEvent.Parser eventParser;
  private BufferedReader objectStream;

  private JsonEvent next;
  private boolean hasNext = true;

  public EventReader(S3Client s3, String bucketName, String prefix, float speedupFactor, String timestampAttributeName) {
    this.s3 = s3;
    this.bucketName = bucketName;
    this.eventParser = new JsonEvent.Parser(speedupFactor, timestampAttributeName);

    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
    this.s3Objects = s3.listObjectsV2Paginator(request).contents().iterator();

    //initialize next and hasNext fields
    next();
  }


  public void seek(Instant timestamp) {
    seek(timestamp, 10_000);
  }


  public void seek(Instant timestamp, int skipNumLines) {
    while (next.timestamp.isBefore(timestamp) && hasNext) {
      //skip skipNumLines before parsing next event
      try {
        for (int i = 0; i < skipNumLines; i++) {
          objectStream.readLine();
        }
      } catch (IOException | NullPointerException e) {
        // if the next line cannot be read, that's fine, the next S3 object will be opened and read by next()
      }

      next();
    }
  }


  @Override
  public boolean hasNext() {
    return hasNext;
  }


  @Override
  public JsonEvent next() {
    String nextLine = null;

    try {
      nextLine = objectStream.readLine();
    } catch (IOException | NullPointerException e) {
      // if the next line cannot be read, that's fine, the next S3 object will be opened and read subsequently
    }

    if (nextLine == null) {
      if (s3Objects.hasNext()) {
        //if another object has been previously read, close it before opening another one
        if (objectStream != null) {
          try {
            objectStream.close();
          } catch (IOException e) {
            LOG.warn("failed to close object: {}", e);
          }
        }

        //try to open the next S3 object
        S3Object s3Object = s3Objects.next();

        //skip objects that obviously don't contain any data; TODO: make this configurable
        if (s3Object.key().endsWith("README.md")) {
          LOG.info("skipping object s3://{}/{}", bucketName, s3Object.key());

          return next();
        }

        LOG.info("reading object s3://{}/{}", bucketName, s3Object.key());

        InputStream stream;
        try {
          GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
          stream = new BufferedInputStream(s3.getObject(request));
        } catch (SdkClientException e) {
          //if we cannot read this object, skip it and try to read the next one
          LOG.warn("skipping object s3://{}/{} as it failed to open", bucketName, s3Object.key());
          LOG.debug("failed to open object", e);

          //if a failure occurs, re-initialize the parser, so that it seems as if we are starting a complete fresh ingestion from the next object
          eventParser.reset();

          return next();
        }

        try {
          stream = new CompressorStreamFactory().createCompressorInputStream(stream);
        } catch (CompressorException e) {
          //if we cannot decompress a stream, that's fine, as it probably is just a stream of uncompressed data
          LOG.info("unable to decompress object: {}", e.getMessage());
        }

        objectStream = new BufferedReader(new InputStreamReader(stream));

        //try to read the next object from the newly opened stream
        return next();
      } else {
        //if there is no next object to parse
        hasNext = false;

        return next;
      }
    } else {
      JsonEvent result = next;

      try {
        //parse the next event and return the current one
          next = eventParser.parse(nextLine);

        return result;
      } catch (IllegalArgumentException e) {
        //if the current line cannot be parsed, just skip it and emit a warning

        LOG.warn("ignoring line: {}", nextLine);

        return next();
      }
    }
  }
}
