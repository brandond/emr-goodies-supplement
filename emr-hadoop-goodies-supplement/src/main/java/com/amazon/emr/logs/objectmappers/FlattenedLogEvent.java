package com.amazon.emr.logs.objectmappers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Timestamp;
import java.util.List;

public class FlattenedLogEvent
{
    private static Gson gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampLongFormatTypeAdapter()).create();
    public String owner;
    public String logGroup;
    public String messageType;
    public List<String> subscriptionFilters;
    public String id;
    public String message;
    public Timestamp timestamp;


    public FlattenedLogEvent(LogRecord record, LogEvent event)
    {
        this.owner = record.getOwner();
        this.logGroup = record.getLogGroup();
        this.messageType = record.getMessageType();
        this.subscriptionFilters = record.getSubscriptionFilters();
        this.id = event.getId();
        this.message = event.getMessage();
        this.timestamp = new Timestamp(event.getTimestamp());
    }

    public static String toJson(FlattenedLogEvent event)
    {
        return gson.toJson(event);
    }

    public static FlattenedLogEvent fromJson(String jsonString)
    {
        return gson.fromJson(jsonString, FlattenedLogEvent.class);
    }

    public String toString() {
        return "FlattenedLogRecord [owner=" + this.owner +
                ", logGroup=" + this.logGroup +
                ", messageType=" + this.messageType +
                ", subscriptionFilters=" + this.subscriptionFilters +
                ", id=" + this.id +
                ", timestamp=" + this.timestamp +
                ", message=" + this.message +
                "]";
    }
}