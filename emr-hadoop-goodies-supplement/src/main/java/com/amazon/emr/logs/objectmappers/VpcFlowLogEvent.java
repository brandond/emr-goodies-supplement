package com.amazon.emr.logs.objectmappers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Timestamp;

public class VpcFlowLogEvent
{
    private static Gson gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampLongFormatTypeAdapter()).create();
    public String version;
    public String account_id;
    public String interface_id;
    public String srcaddr;
    public String dstaddr;
    public Integer srcport;
    public Integer dstport;
    public Integer protocol;
    public Long packets;
    public Long bytes;
    public Timestamp start;
    public Timestamp end;
    public String action;
    public String log_status;

    public VpcFlowLogEvent(String str)
    {
        String[] parts = str.split(" ");
        if (parts.length != 14){
            throw new IllegalArgumentException("Invalid VPC Flow Log message length");
        }

        version = parts[0];
        account_id = parts[1];
        interface_id = parts[2];
        srcaddr = parts[3];
        dstaddr = parts[4];
        srcport = tryParseInt(parts[5]);
        dstport = tryParseInt(parts[6]);
        protocol = tryParseInt(parts[7]);
        packets = tryParseLong(parts[8]);
        bytes = tryParseLong(parts[9]);
        start = tryParseTimestamp(parts[10]);
        end = tryParseTimestamp(parts[11]);
        action = parts[12];
        log_status = parts[13];
    }

    public static String toJson(VpcFlowLogEvent event)
    {
        return gson.toJson(event);
    }

    public static VpcFlowLogEvent fromJson(String jsonString)
    {
        return gson.fromJson(jsonString, VpcFlowLogEvent.class);
    }

    private static Integer tryParseInt(String str)
    {
        try {
            return Integer.parseUnsignedInt(str);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Long tryParseLong(String str)
    {
        try {
            return Long.parseUnsignedLong(str);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Timestamp tryParseTimestamp(String str)
    {
        try {
            return new Timestamp(Long.parseUnsignedLong(str) * 1000);
        } catch (NumberFormatException ex){
            return null;
        }
    }
}