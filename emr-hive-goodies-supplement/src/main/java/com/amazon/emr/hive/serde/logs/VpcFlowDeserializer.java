package com.amazon.emr.hive.serde.logs;

import com.amazon.emr.logs.objectmappers.CloudWatchLogEntry;
import com.amazon.emr.logs.objectmappers.LogEventException;
import com.amazon.emr.logs.objectmappers.VpcFlowLogEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class VpcFlowDeserializer
    extends AbstractDeserializer
{
    private static final Log LOG = LogFactory.getLog(VpcFlowDeserializer.class);
    private Set<String> loggedInvalidGroups = new HashSet<String>();
    private ObjectInspector objectInspector;

    public void initialize(Configuration conf, Properties tbl)
        throws SerDeException
    {
        this.objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(VpcFlowLogEvent.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    public Object deserialize(Writable blob)
        throws SerDeException
    {
        CloudWatchLogEntry entry = (CloudWatchLogEntry) blob;
        try {
            return entry.getVpcFlowLogEvent();
        } catch (LogEventException e) {
            String group = entry.getEvent().logGroup;
            if (! loggedInvalidGroups.contains(group)) {
                LOG.warn("Skipping invalid VPC Flow Log event from Log Group " + group);
                loggedInvalidGroups.add(group);
            }
            return null;
        }
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
