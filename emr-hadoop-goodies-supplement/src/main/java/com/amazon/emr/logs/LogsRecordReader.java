package com.amazon.emr.logs;

import com.amazon.emr.hadoop.MemberRecordReader;
import com.amazon.emr.logs.objectmappers.CloudWatchLogEntry;
import com.amazon.emr.logs.objectmappers.FlattenedLogEvent;
import com.amazon.emr.logs.objectmappers.LogRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class LogsRecordReader
    implements RecordReader<LongWritable, CloudWatchLogEntry>
{
    public static final Log LOG = LogFactory.getLog(LogsRecordReader.class);
    private MemberRecordReader recordReader;
    private LogRecord logRecord;
    private int listPos = 0;
    Text lineValue = new Text();

    public LogsRecordReader(FileSplit split, Configuration conf, Reporter reporter)
        throws IOException
    {
        this.recordReader = new MemberRecordReader(conf, split);
        this.logRecord = new LogRecord();
    }

    public void close()
        throws IOException
    {
        this.recordReader.close();
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public CloudWatchLogEntry createValue() {
        return new CloudWatchLogEntry();
    }

    public long getPos()
        throws IOException
    {
        return this.recordReader.getPos();
    }

    public float getProgress()
        throws IOException
    {
        return this.recordReader.getProgress();
    }

    public boolean next(LongWritable key, CloudWatchLogEntry value)
        throws IOException
    {
        if (this.listPos < this.logRecord.getLogEvents().size())
        {
            value.setEvent(new FlattenedLogEvent(this.logRecord, this.logRecord.getLogEvents().get(this.listPos++)));
            return true;
        }

        while (this.recordReader.next(key, this.lineValue)) {
            this.listPos = 0;
            this.logRecord = new LogRecord();

            try
            {
                this.logRecord.populateObjectFromJson(this.lineValue.toString());
            }
            catch (Exception e)
            {
                LOG.error("Encountered an exception while parsing CloudWatch log entry. Ignoring it and moving to next record.", e);
                continue;
            }

            if (this.logRecord.getLogEvents().size() > 0)
            {
                value.setEvent(new FlattenedLogEvent(this.logRecord, this.logRecord.getLogEvents().get(this.listPos++)));
                return true;
            }
        }
        return false;
    }
}
