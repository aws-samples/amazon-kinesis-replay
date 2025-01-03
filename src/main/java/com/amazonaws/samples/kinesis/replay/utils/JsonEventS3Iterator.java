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

package com.amazonaws.samples.kinesis.replay.utils;

import com.amazonaws.samples.kinesis.replay.events.JsonEvent;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.*;
import java.time.Instant;
import java.util.Iterator;

/**
 * Iterates JsonEvents reading from an S3 prefix.
 */
public class JsonEventS3Iterator implements Iterator<JsonEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonEventS3Iterator.class);

    private final String bucketName;
    private final S3Client s3;
    private final Iterator<S3Object> s3Objects;
    private final String objectSuffixToSkip;

    private final JsonEvent.Parser eventParser;
    private BufferedReader objectStream;

    private JsonEvent next;
    private boolean hasNext = true;


    public JsonEventS3Iterator(S3Client s3, String bucketName, String prefix, float speedupFactor, String timestampAttributeName, String objectSuffixToSkip) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.eventParser = new JsonEvent.Parser(speedupFactor, timestampAttributeName);
        this.objectSuffixToSkip = objectSuffixToSkip;
        LOG.debug("Iterating over s3://{}/{}, skipping objects named '{}'", bucketName, prefix, objectSuffixToSkip);


        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
        this.s3Objects = s3.listObjectsV2Paginator(request).contents().iterator();

        // initialize next and hasNext fields
        next();
    }

    /**
     * Reader skipping README.md files
     */
    public JsonEventS3Iterator(S3Client s3, String bucketName, String prefix, float speedupFactor, String timestampAttributeName) {
        this(s3, bucketName, prefix, speedupFactor, timestampAttributeName, "README.md");
    }


    /**
     * Seek an event reader to a timestamp, reading through the S34 objects, skipping 10000 rows on each read.
     */
    public void seek(Instant timestamp) {
        seek(timestamp, 10_000);
    }

    /**
     * Seek an event reader to a timestamp, reading through the S34 objects, skipping skipNumLines rows on each read.
     */
    public void seek(Instant timestamp, int skipNumLines) {
        LOG.info("Seek {}, reading every {} rows", timestamp, skipNumLines);
        while (next.timestamp.isBefore(timestamp) && hasNext) {
            // Skip skipNumLines before parsing next event
            try {
                for (int i = 0; i < skipNumLines; i++) {
                    objectStream.readLine();
                }
            } catch (IOException | NullPointerException e) {
                LOG.debug("Reached end of an S3 object during seek");
                // if the next line cannot be read, that's fine, the next S3 object will be opened and read by next()
            }

            next();
        }
    }


    @Override
    public boolean hasNext() {
        return hasNext;
    }


    /**
     * Read the next JsonEvent
     */
    @Override
    public JsonEvent next() {
        String nextLine = null;

        try {
            nextLine = objectStream.readLine();
        } catch (IOException | NullPointerException e) {
            LOG.debug("Reached end of an S3 object");
            // if the next line cannot be read, that's fine, the next S3 object will be opened and read subsequently
        }

        if (nextLine == null) {
            if (s3Objects.hasNext()) {
                // If another object has been previously read, close it before opening another one
                if (objectStream != null) {
                    try {
                        objectStream.close();
                    } catch (IOException e) {
                        LOG.warn("Failed to close an S3 object", e);
                    }
                }

                // Try to open the next S3 object
                S3Object s3Object = s3Objects.next();

                // Skip objects that obviously don't contain any data
                if (s3Object.key().endsWith(objectSuffixToSkip)) {
                    LOG.info("Skipping object s3://{}/{}", bucketName, s3Object.key());

                    return next();
                }

                LOG.info("Reading from s3://{}/{}", bucketName, s3Object.key());

                InputStream stream;
                try {
                    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
                    stream = new BufferedInputStream(s3.getObject(request));
                } catch (SdkClientException e) {
                    //if we cannot read this object, skip it and try to read the next one
                    LOG.warn("Skipping object s3://{}/{} as it failed to open", bucketName, s3Object.key());
                    LOG.debug("Failed to open object", e);

                    // if a failure occurs, re-initialize the parser, so that it seems as if we are starting a complete fresh ingestion from the next object
                    eventParser.reset();

                    return next();
                }

                try {
                    stream = new CompressorStreamFactory().createCompressorInputStream(stream);
                } catch (CompressorException e) {
                    //if we cannot decompress a stream, that's fine, as it probably is just a stream of uncompressed data
                    LOG.info("Unable to decompress object: {}", e.getMessage());
                }

                objectStream = new BufferedReader(new InputStreamReader(stream));

                // try to read the next object from the newly opened stream
                return next();
            } else {
                // if there is no next object to parse
                hasNext = false;

                return next;
            }
        } else {
            JsonEvent result = next;

            try {
                // parse the next event and return the current one
                next = eventParser.parse(nextLine);

                return result;
            } catch (IllegalArgumentException e) {
                // if the current line cannot be parsed, just skip it and emit a warning

                LOG.warn("Ignoring row (unable to parse): {}", nextLine);

                return next();
            }
        }
    }
}
