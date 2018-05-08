package com.amazon.emr.logs.objectmappers;

public class LogEventException extends Exception {
    public LogEventException(){
    }

    public LogEventException(String message){
        super(message);
    }

    public LogEventException(Throwable throwable){
        super(throwable);
    }

    public LogEventException(String message, Throwable throwable){
        super(message, throwable);
    }
}
