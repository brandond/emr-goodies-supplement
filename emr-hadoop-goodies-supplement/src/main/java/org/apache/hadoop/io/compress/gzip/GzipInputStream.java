package org.apache.hadoop.io.compress.gzip;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class GzipInputStream extends InputStream {
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

    READ_MODE readMode = READ_MODE.CONTINUOUS;
    private int bytesReadFromCompressedStream = 0;
    private int bytesLastRead = 0;
    private byte[] array = new byte[DEFAULT_DIRECT_BUFFER_SIZE];
    private BufferedInputStream in;
    private Decompressor decompressor;

    public static final int END_OF_BLOCK = -2;
    public static final int END_OF_STREAM = -1;

    public GzipInputStream(final InputStream in, Decompressor decompressor, READ_MODE readMode)
    {
        super();
        this.in = new BufferedInputStream(in);
        this.decompressor = decompressor;
    }

    public GzipInputStream(final InputStream in, Decompressor decompressor)
    {
        this(in, decompressor, READ_MODE.CONTINUOUS);
    }

    public int read()
        throws IOException
    {
        if (this.in != null) {
            int result = this.read(array, 0, 1);
            return (result < 0) ? result : (array[0] & 0xff);
        } else {
            throw new IOException("Stream closed");
        }
    }

    public int getProcessedByteCount() {
        return this.bytesReadFromCompressedStream;
    }

    protected void updateProcessedByteCount(int count) {
        this.bytesReadFromCompressedStream += count;
    }

    public int read(final byte[] dest, final int offs, final int len)
        throws IOException
    {
        if (offs < 0) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") < 0.");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException("len(" + len + ") < 0.");
        }
        if (offs + len > dest.length) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") + len("
                    + len + ") > dest.length(" + dest.length + ").");
        }
        if (this.in == null) {
            throw new IOException("Stream closed");
        }

        if (decompressor.needsInput()) {
            in.mark(array.length);
            bytesLastRead = in.read(array, 0, array.length);
            if (bytesLastRead > 0) {
                decompressor.setInput(array, 0, bytesLastRead);
                updateProcessedByteCount(bytesLastRead);
            }
        }

        int readSize = decompressor.decompress(dest, offs, len);
        if (readSize == 0 && decompressor.finished()) {
            int remaining = decompressor.getRemaining();
            if (remaining > 0) {
                in.reset();
                long skippedBytes = in.skip(bytesLastRead - remaining);
                if (skippedBytes < (bytesLastRead - remaining)) {
                    throw new IOException("Failed to reset stream to start of next block");
                }
                decompressor.reset();
                updateProcessedByteCount(-remaining);
                return END_OF_BLOCK;
            } else {
                return END_OF_STREAM;
            }
        }
        return readSize;
    }
}
