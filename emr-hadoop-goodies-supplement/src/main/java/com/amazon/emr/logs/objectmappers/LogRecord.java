package com.amazon.emr.logs.objectmappers;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;

public class LogRecord
{
    private static Gson gson = new Gson();
    private String owner;
    private String logGroup;
    private String logStream;
    private String messageType;
    private List<String> subscriptionFilters;
    private List<LogEvent> logEvents;

    public LogRecord()
    {
        this.subscriptionFilters = Lists.newArrayList();
        this.logEvents = Lists.newArrayList();
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getLogGroup() {
        return logGroup;
    }

    public void setLogGroup(String logGroup) {
        this.logGroup = logGroup;
    }

    public String getLogStream() {
        return logStream;
    }

    public void setLogStream(String logStream) { this.logStream = logStream; }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public List<String> getSubscriptionFilters() {
        return subscriptionFilters;
    }

    public void setSubscriptionFilters(List<String> subscriptionFilters) {
        this.subscriptionFilters = subscriptionFilters;
    }

    public List<LogEvent> getLogEvents()
    {
        return this.logEvents;
    }

    public void setLogEvents(List<LogEvent> logEvents)
    {
        this.logEvents = logEvents;
    }

    public void populateObjectFromJson(String json){
        LogRecord logRecord = (LogRecord)gson.fromJson(json, LogRecord.class);
        setOwner(logRecord.getOwner());
        setLogGroup(logRecord.getLogGroup());
        setLogStream(logRecord.getLogStream());
        setMessageType(logRecord.getMessageType());
        setSubscriptionFilters(logRecord.getSubscriptionFilters());
        setLogEvents(logRecord.getLogEvents());
    }

    public String toString()
    {
        return "LogRecord [owner=" + this.owner +
                ", logGroup=" + this.logGroup +
                ", logStream=" + this.logStream +
                ", messageType=" + this.messageType +
                ", subscriptionFilters=" + this.subscriptionFilters +
                ", logEvents=" + this.logEvents +
                "]";
    }
}