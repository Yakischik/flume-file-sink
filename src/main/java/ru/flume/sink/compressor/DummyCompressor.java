package ru.flume.sink.compressor;

import java.io.OutputStream;

import org.apache.flume.Context;

/**
 * Compressor, не использующий сжатия. Пишет данные "как есть".
 */
public class DummyCompressor extends Compressor {

    public DummyCompressor(Context context) {
        super(context);
    }

    public OutputStream wrap(OutputStream out) {
        return out;
    }
}
