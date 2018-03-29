package com.amazon.emr.hive.serde.logs;

import com.amazon.emr.logs.objectmappers.CloudWatchLogEntry;
import com.amazon.emr.logs.objectmappers.FlattenedLogEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

public class LogsDeserializer
    extends AbstractDeserializer
{
    private ObjectInspector objectInspector;

    public void initialize(Configuration conf, Properties tbl)
        throws SerDeException
    {
        this.objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(FlattenedLogEvent.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    public Object deserialize(Writable blob)
        throws SerDeException
    {
        CloudWatchLogEntry entry = (CloudWatchLogEntry)blob;
        return entry.getEvent();
    }

    public ObjectInspector getObjectInspector()
        throws SerDeException
    {
        return this.objectInspector;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}
