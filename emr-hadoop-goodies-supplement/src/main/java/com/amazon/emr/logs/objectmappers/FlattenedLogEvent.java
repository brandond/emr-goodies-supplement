package com.amazon.emr.logs.objectmappers;

import com.google.gson.Gson;
import java.util.List;

public class FlattenedLogEvent
{
    private static Gson gson = new Gson();
    private String owner;
    private String logGroup;
    private String messageType;
    private List<String> subscriptionFilters;
    private String id;
    private String message;
    private long timestamp;

    public FlattenedLogEvent(LogRecord record, LogEvent event)
    {
        this.owner = record.getOwner();
        this.logGroup = record.getLogGroup();
        this.messageType = record.getMessageType();
        this.subscriptionFilters = record.getSubscriptionFilters();
        this.id = event.getId();
        this.message = event.getMessage();
        this.timestamp = event.getTimestamp();
    }

    public static String toJson(FlattenedLogEvent event)
    {
        return gson.toJson(event);
    }

    public static FlattenedLogEvent fromJson(String jsonString)
    {
        return (FlattenedLogEvent)gson.fromJson(jsonString, FlattenedLogEvent.class);
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