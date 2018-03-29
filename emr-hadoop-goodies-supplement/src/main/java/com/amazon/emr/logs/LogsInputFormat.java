package com.amazon.emr.logs;

import com.amazon.emr.logs.objectmappers.CloudWatchLogEntry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class LogsInputFormat
    extends FileInputFormat<LongWritable, CloudWatchLogEntry>
{
    @Override
    public RecordReader<LongWritable, CloudWatchLogEntry> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
        throws IOException
    {
        return new LogsRecordReader((FileSplit)split, job, reporter);
    }
}
