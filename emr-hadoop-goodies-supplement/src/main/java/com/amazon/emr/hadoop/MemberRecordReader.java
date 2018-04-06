package com.amazon.emr.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Treats keys as offset reader compressed file and value as a single chunk of text.
 */

@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class MemberRecordReader
    implements RecordReader<LongWritable, Text>
{
    private static final Log LOG = LogFactory.getLog(MemberRecordReader.class.getName());
    private static final String CODEC_FORCE = "codec.force";
    private CompressionCodecFactory compressionCodecs;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private MemberReader reader;
    private Seekable inputStream;
    private int maxLineLength;
    private FileSplit fileSplit;

    public MemberRecordReader(Configuration job, FileSplit split)
        throws IOException
    {
        fileSplit = split;
        if (fileSplit.getStart() != 0) {
            throw new IOException("Splitting not supported");
        }

        maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
        compressionCodecs = new CompressionCodecFactory(job);

        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        final String codecName = job.get(CODEC_FORCE);

        if (codecName != null)
        {
            codec = compressionCodecs.getCodecByName(codecName);
        } else {
            codec = compressionCodecs.getCodec(file);
        }

        final Seekable openStream = fs.open(file);
        if (codec != null)
        {
            decompressor = CodecPool.getDecompressor(codec);
            inputStream = codec.createInputStream((InputStream)openStream, decompressor);
        } else {
            inputStream = openStream;
        }
        reader = new MemberReader((InputStream)inputStream, job);
    }

    public synchronized boolean next(LongWritable key, Text value)
        throws IOException
    {
        key.set(inputStream.getPos());
        return reader.readLine(value, maxLineLength) > 0;
    }

    public LongWritable createKey()
    {
        return new LongWritable();
    }

    public Text createValue()
    {
        return new Text();
    }

    public synchronized long getPos()
        throws IOException
    {
        return inputStream.getPos();
    }

    public synchronized float getProgress()
        throws IOException
    {
        return ((float)inputStream.getPos() / fileSplit.getLength());
    }

    public synchronized void close()
        throws IOException
    {
        try {
            if (reader != null) {
                reader.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }
}
