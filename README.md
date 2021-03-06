emr-goodies-supplement
======================

### Even more extensions for EMR

AWS provides some 'extensions' for EMR to make your life easier when processing logs produced by Amazon services.
Specifically, `emr-hadoop-goodies.jar` and `emr-hive-goodies.jar` provide InputFormats and SerDes that 
allow you to directly query CloudTrail logs stored in S3. You'll find references to these classes peppered throughout
[the documentation](https://docs.aws.amazon.com/athena/latest/ug/cloudtrail.html):

```sql
CREATE EXTERNAL TABLE cloudtrail_logs (
(...)
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
(...)
```

There are some additional goodies in there for parsing S3 (`com.amazon.emr.hive.serde.s3.S3LogDeserializer`) and EMR (`com.amazon.hive.serde.EmrServiceLogSerDe`, `com.amazon.hive.inputformat.ServiceLogInputFormat`) logs, but I've never found any public references to them so they're probably underutilized.

In This Repo
------------
This repo contains code for additional classes to support parsing of CloudWatch Logs messages and other data [streamed to S3 via Kinesis Firehose](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html#FirehoseExample). These classes include:
* `com.amazon.emr.logs.LogsInputFormat` - Input format for CloudWatch Logs records
* `com.amazon.emr.hive.serde.logs.LogsDeserializer` - Deserializer for Cloudwatch Logs records
* `com.amazon.emr.hive.serde.logs.VpcFlowDeserializer` - Deserializer for Cloudwatch Logs records containing VPC Flow Logs
* `com.amazon.emr.hadoop.MemberRecordReader` - Configurable file-record based record reader that supports overriding compression codec selection (intended for use with `SplittableGzipCodec`)
* `org.apache.hadoop.io.compress.SplittableGzipCodec` - Compression codec that reads multiple concatenated gzip chunks as individual records (as output by CloudWatch Logs)

In order to use these classes, you will need to:
* Build the code from this repo.
* Copy `emr-hadoop-goodies-supplement-2.9.0.jar` and `emr-hive-goodies-supplement-2.9.0.jar` to `/usr/share/aws/emr/lib/` on your EMR nodes.
* Add `org.apache.hadoop.io.compress.SplittableGzipCodec` to the begining of the list in the `io.compression.codecs` property within `/etc/hadoop/conf.empty/core-site.xml`

A bootstrap action to perform these steps will be available soon.

Example DDL
-----------
**CloudWatch Logs:**
```sql
CREATE EXTERNAL TABLE `cloudwatch_logs`
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.logs.LogsDeserializer'
STORED AS INPUTFORMAT 'com.amazon.emr.logs.LogsInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mys3bucket/'
TBLPROPERTIES ('codec.force'='splittablegzip')
```

**VPC Flow Logs:**
```sql
CREATE EXTERNAL TABLE `vpc_flow_logs`
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.logs.VpcFlowDeserializer'
STORED AS INPUTFORMAT 'com.amazon.emr.logs.LogsInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mys3bucket/'
TBLPROPERTIES ('codec.force'='splittablegzip')
```

Notes
-----

`com.amazon.emr.hive.serde.s3.S3LogDeserializer` seems to be exactly the same as `org.apache.hadoop.hive.contrib.serde2.s3.S3LogDeserializer`, right down to the weird `S3ZemantaDeserializer[]` stringification, so it's possible that some of the closed-source 'goodies' are actually covered by an Apache license. See:
 https://forums.aws.amazon.com/thread.jspa?messageID=628236
