package com.amazon.emr.logs.objectmappers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CloudWatchLogEntry
    implements Writable
{
    private FlattenedLogEvent event;

    public FlattenedLogEvent getEvent() {
        return event;
    }

    public void setEvent(FlattenedLogEvent event) {
        this.event = event;
    }

    public void write(DataOutput out)
        throws IOException
    {
        out.writeUTF(FlattenedLogEvent.toJson(this.event));
    }

    public void readFields(DataInput in)
            throws IOException
    {
        this.event = FlattenedLogEvent.fromJson(in.readUTF());
    }
}