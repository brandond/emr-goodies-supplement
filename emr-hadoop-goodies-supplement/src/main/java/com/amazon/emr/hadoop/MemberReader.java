package com.amazon.emr.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MemberReader implements Closeable {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;

    public MemberReader(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
    }

    public MemberReader(InputStream in, Configuration conf)
            throws IOException
    {
        this(in, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE));
    }

    public MemberReader(InputStream in)
    {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    public void close()
        throws IOException
    {
        in.close();
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
        throws IOException
    {

        int totalBytesRead = 0;
        int lastBytesRead = 0;
        int nextReadSize = bufferSize;
        str.clear();

        while (totalBytesRead < maxBytesToConsume) {
            if (totalBytesRead + nextReadSize > maxBytesToConsume) {
                nextReadSize = maxBytesToConsume - totalBytesRead;
            }

            if (nextReadSize > 0) {
                lastBytesRead = in.read(buffer, 0, nextReadSize);
            } else {
                lastBytesRead = 0;
            }

            if (lastBytesRead > 0) {
                totalBytesRead += lastBytesRead;
                if (str.getLength() + lastBytesRead > maxLineLength) {
                    lastBytesRead = maxLineLength - str.getLength();
                }
                str.append(buffer, 0, lastBytesRead);
            } else {
                break;
            }
        }

        return totalBytesRead;
    }

    public int readLine(Text str, int maxLineLength)
        throws IOException
    {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    public int readLine(Text str)
        throws IOException
    {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
}
