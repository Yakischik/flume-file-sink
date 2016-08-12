package ru.flume.sink.compressor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.flume.Context;

/**
 * Compressor, использующий алгоритм сжатия GZIP.
 */
public class GzipCompressor extends Compressor {
   
    private static final String PARAM_SYNC_FLUSH = "sink.compressor.gzip.syncFlush";
    private static final String PARAM_BUFFER_SIZE= "sink.compressor.gzip.bufferSize";
    
    private final boolean syncFlush;
    private final int bufferSize;

    public GzipCompressor(Context context) {
        super(context);
        this.syncFlush = context.getBoolean(PARAM_SYNC_FLUSH, false);
        this.bufferSize = context.getInteger(PARAM_BUFFER_SIZE, 512);
    }

    @Override
    public OutputStream wrap(OutputStream out) throws IOException {
        return new GZIPOutputStream(out, bufferSize, syncFlush);
    }

    @Override
    public String getExtension() {
        return super.getExtension() + ".gz";
    }
}
