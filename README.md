## Amazon Kinesis Replay

A simple Java application that replays Json events that are stored in objects in Amazon S3 into a Amazon Kinesis stream. The application reads the timestamp attribute of the stored events and replays them as if they occurred in real time.

### Building

Install maven:
```
brew install maven
```

Build the package:
```
mvn package
```

### Original README continued

By default, the application will replay a historic data set of taxi trips that made in New York City that is derived from the public dataset [available from the Registry of Open Data on AWS](https://registry.opendata.aws/nyc-tlc-trip-records-pds/).

```
$ java -jar amazon-kinesis-replay-1.0.jar -streamName «Kinesis stream name» -streamRegion «AWS region»
```

To increase the number of events sent per second, you can accelerate the replay using the `-speedup` parameter.

The following command replays one hour of data within one second.

```
$ java -jar amazon-kinesis-replay-1.0.jar -streamName «Kinesis stream name» -streamRegion «AWS region» -speedup 3600
```

To aggregate multiple events in a Kinesis Data Streams record, you can use the `-aggregate` option. Aggregation allows you to increase the number of records sent per API call, which effectively increases producer throughput

```
$ java -jar amazon-kinesis-replay-1.0.jar -streamName «Kinesis stream name» -streamRegion «AWS region» -speedup 3600 -aggregate
```

To specify an alternative dataset you can use the `-bucket` and `-prefix` options as long as the events in the objects are stored in minified Json format, have a `timestamp` attribute and are ordered by this timestamp. The name of the timestamp attribute can be customized with the `timestampAttributeName` parameter.

```
$ java -jar amazon-kinesis-replay-1.0.jar -streamName «Kinesis stream name» -streamRegion «AWS region» -bucketName «S3 bucket name» -bucketRegion «S3 bucket region» -objectPrefix «S3 prefix of objects to read»
```

More options can be obtained through the `-help` parameter.

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.
