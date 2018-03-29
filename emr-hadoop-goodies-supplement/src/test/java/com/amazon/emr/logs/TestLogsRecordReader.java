package com.amazon.emr.logs;

import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.mapred.Reporter.NULL;

import com.amazon.emr.logs.objectmappers.CloudWatchLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

public class TestLogsRecordReader {
    private static Path workDir = new Path(new Path(System.getProperty(
            "test.build.data", "target"), "data"), "TestTextInputFormat");
    private static Path inputDir = new Path(workDir, "input");

    public ArrayList<String> readRecords(URL testFileUrl, int splitSize, Boolean forceGzip)
        throws IOException
    {
        // Set up context
        File testFile = new File(testFileUrl.getFile());
        long testFileSize = testFile.length();
        Path testFilePath = new Path(testFile.getAbsolutePath());
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", 1);
        conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.SplittableGzipCodec,org.apache.hadoop.io.compress.GzipCodec");
        if (forceGzip)
        {
            conf.set("textfile.compress", "splittablegzip");
        }

        ArrayList<String> records = new ArrayList<String>();

        long offset = 0;
        LongWritable key = new LongWritable();
        CloudWatchLogEntry value = new CloudWatchLogEntry();
        while (offset < testFileSize) {
            FileSplit split = new FileSplit(testFilePath, offset, splitSize, (String[]) null);
            LogsRecordReader reader = new LogsRecordReader(split, conf, NULL);

            while (reader.next(key, value)) {
                records.add(value.toString());
            }
            offset += splitSize;
        }
        return records;
    }

    private void checkSampleRecord(String testFile, int eventCount, Boolean forceGzip)
        throws IOException
    {
        URL testFileUrl = getClass().getClassLoader().getResource(testFile);
        ArrayList<String> records = readRecords(testFileUrl, 1024, forceGzip);
        assertEquals("Wrong number of records", eventCount, records.size());
    }

    @Test
    public void testLoadingSampleRecords()
        throws IOException
    {
        checkSampleRecord("CloudWatchLogsRecord.json", 2, false);
    }

    @Test
    public void testLoadingCompressedRecords()
        throws IOException
    {
        checkSampleRecord("CloudWatchLogsRecord.json.gz", 2, false);
    }

    @Test
    public void testLoadingMultipleCompressedRecords()
        throws IOException
    {
        checkSampleRecord("CloudWatchLogsRecordsMultiGzip", 4, true);
    }
}
