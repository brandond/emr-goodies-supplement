package com.amazon.emr.logs.objectmappers;

import com.google.gson.Gson;

public class LogEvent {
    private String id;
    private String message;
    private long timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String toString()
    {
        return "LogEvent [id=" + this.id +
                ", timestamp=" + this.timestamp +
                ", message=" + this.message +
                "]";
    }
}
