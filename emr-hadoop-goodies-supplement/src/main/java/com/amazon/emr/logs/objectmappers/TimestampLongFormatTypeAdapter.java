package com.amazon.emr.logs.objectmappers;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.Timestamp;

public class TimestampLongFormatTypeAdapter extends TypeAdapter<Timestamp> {
    public void write(JsonWriter out, Timestamp value)
            throws IOException
    {
        out.value(value.getTime());
    }

    public Timestamp read(JsonReader in)
            throws IOException
    {
        return new Timestamp(in.nextLong());
    }
}
