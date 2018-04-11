package com.amazon.emr.logs.objectmappers;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CloudWatchLogEntry
    implements Writable
{
    private FlattenedLogEvent event;

    public VpcFlowLogEvent getVpcFlowLogEvent() {
        return new VpcFlowLogEvent(event.message);
    }

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

    public String toString()
    {
        return FlattenedLogEvent.toJson(this.event);
    }
}