package org.apache.hadoop.io.compress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.compress.gzip.GzipInputStream;

import java.io.BufferedInputStream;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

public class SplittableGzipCodec
    implements Configurable, SplittableCompressionCodec
{
    private static final Log LOG = LogFactory.getLog(SplittableGzipCodec.class.getName());
    private Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public SplittableGzipCodec(){ }

    public CompressionOutputStream createOutputStream(OutputStream out)
        throws IOException
    {
        if (!ZlibFactory.isNativeZlibLoaded(conf)) {
            return new GzipCodec.GzipOutputStream(out);
        }
        return CompressionCodec.Util.
                createOutputStreamWithCodecPool(this, conf, out);
    }

    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
        throws IOException
    {
        return (compressor != null) ?
                new CompressorStream(out, compressor,
                        conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT)) :
                createOutputStream(out);
    }

    public Class<? extends Compressor> getCompressorType()
    {
        return ZlibFactory.isNativeZlibLoaded(conf)
                ? GzipCodec.GzipZlibCompressor.class
                : null;
    }

    public Compressor createCompressor()
    {
        return (ZlibFactory.isNativeZlibLoaded(conf))
                ? new GzipCodec.GzipZlibCompressor(conf)
                : null;
    }

    public CompressionInputStream createInputStream(InputStream in)
        throws IOException
    {
        return CompressionCodec.Util.createInputStreamWithCodecPool(this, conf, in);
    }

    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
        throws IOException
    {
        return new GzipCompressionInputStream(in, decompressor, 0L, Long.MAX_VALUE, READ_MODE.BYBLOCK);
    }

    public SplitCompressionInputStream createInputStream(InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode)
        throws IOException
    {
        if (!(seekableIn instanceof Seekable)) {
            throw new IOException("seekableIn must be an instance of " + Seekable.class.getName());
        }

        ((Seekable)seekableIn).seek(start);
        return new GzipCompressionInputStream(seekableIn, decompressor, start, end, readMode);
    }

    public Class<? extends Decompressor> getDecompressorType()
    {
        return ZlibFactory.isNativeZlibLoaded(conf) ?
                ZlibDecompressor.class : BuiltInGzipDecompressor.class;
    }

    public Decompressor createDecompressor()
    {
        return ZlibFactory.isNativeZlibLoaded(conf) ?
                new ZlibDecompressor(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB,
                                     conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT)) :
                new BuiltInGzipDecompressor();
    }

    public String getDefaultExtension()
    {
        return ".zcat";
    }

    private static class GzipCompressionInputStream extends SplitCompressionInputStream
    {
        private GzipInputStream input;
        private Decompressor decompressor;
        boolean needsReset;
        private BufferedInputStream bufferedIn;
        private READ_MODE readMode = READ_MODE.CONTINUOUS;
        private long startingPos = 0;

        private enum POS_ADVERTISEMENT_STATE_MACHINE {
            HOLD, ADVERTISE
        };

        POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
        long compressedStreamPosition = 0;

        public GzipCompressionInputStream(InputStream in, Decompressor decompressor)
            throws IOException
        {
            this(in, decompressor, 0L, Long.MAX_VALUE, READ_MODE.CONTINUOUS);
        }

        public GzipCompressionInputStream(InputStream in, Decompressor decompressor, long start, long end, READ_MODE readMode)
            throws IOException
        {
            super(in, start, end);
            needsReset = false;
            bufferedIn = new BufferedInputStream(super.in);
            this.decompressor = decompressor;
            this.startingPos = super.getPos();
            this.readMode = readMode;

            input = new GzipInputStream(bufferedIn, this.decompressor, this.readMode);

            if (!(this.readMode == READ_MODE.BYBLOCK && this.startingPos == 0)) {
                this.updatePos();
            }
        }

        public void close()
            throws IOException
        {
            if (!needsReset)
            {
                try {
                    input.close();
                    needsReset = true;
                } finally {
                    super.close();
                }
            }
        }

        public int read(byte[] b, int off, int len)
            throws IOException
        {
            if (needsReset) {
                internalReset();
            }

            int result = this.input.read(b, off, len);
            if (result == GzipInputStream.END_OF_BLOCK) {
                this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
            }

            if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
                this.updatePos();
                this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
            }

            return result;
        }

        public int read()
            throws IOException
        {
            byte b[] = new byte[1];
            int result = this.read(b, 0, 1);
            return (result < 0) ? result : (b[0] & 0xff);
        }

        private void internalReset()
            throws IOException
        {
            if (needsReset) {
                needsReset = false;
                input = new GzipInputStream(bufferedIn, this.decompressor, this.readMode);
            }
        }

        public void resetState()
            throws IOException
        {
            needsReset = true;
        }

        public long getPos()
        {
            return this.compressedStreamPosition;
        }

        private void updatePos() {
            this.compressedStreamPosition = this.startingPos + this.input.getProcessedByteCount();
        }
    }
}
