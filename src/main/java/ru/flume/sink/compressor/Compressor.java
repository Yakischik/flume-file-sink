
package ru.flume.sink.compressor;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Класс, предоставляющий поток для записи, использующий (или не использующий) некоторый алгоритм сжатия.
 */
public abstract class Compressor {

    private static final Logger LOG = LoggerFactory.getLogger(Compressor.class);
    
    public static final Map<String, Class> ALIASES = new HashMap<String, Class>() {{
        put("gzip", GzipCompressor.class);
        put("text", DummyCompressor.class);
    }};
    
    private static final String PARAM_COMPRESSOR = "sink.compressor";
    private static final String PARAM_COMPRESSOR_EXTENSION = "sink.extension";
    
    private static final String DEFAULT_EXTENSION = ".log";
    
    protected final String extension;
    
//----------------------------------------//
    /**
     * Создает Compressor на основе файла конфигурации Flume. В зависимости от выбранного алгоритма сжатия, требуемый
     * набор параметров в конфигурации может разниться.
     */    
    public Compressor(Context context) {
        String ext = context.getString(PARAM_COMPRESSOR_EXTENSION, DEFAULT_EXTENSION);
        if (!ext.startsWith(".")) {
            ext = "." + ext;
        }
        this.extension = ext;
        LOG.info("File extension: {}", this.extension);
    }   
    
//----------------------------------------//
    /**
     * Предоставляет поток для записи, использующий сжатие.
     * 
     * @param out   исходный поток на запись
     * @throws IOException  если невозможно создать поток для записи
     */
    public abstract OutputStream wrap(OutputStream out) throws IOException;
    
//----------------------------------------//    
    /**
     * @return расширение файла, соответствующее типу сжатия
     */
    public String getExtension() {
        return extension;
    }
    
//----------------------------------------//
    /**
     * Создает конкретную реализацию Compressor'а на основе конфигурации Flume.
     */
    public static Compressor createCompressor(Context context) {
        String compressor = context.getString(PARAM_COMPRESSOR, null);
        if (compressor == null) {
            LOG.info("Compressor for files are not defined, writing data as text");
            return new DummyCompressor(context);
        }
        Class clazz = ALIASES.get(compressor);
        if (clazz == null) {
            try {
                clazz = Class.forName(compressor);
            } catch (ClassNotFoundException ex) {
                LOG.error("Compressor class '{}' not found, data will be written without compression", compressor);
                return new DummyCompressor(context);
            }
        }
        try {
            Object result = clazz.getConstructor(Context.class).newInstance(context);
            if (result instanceof Compressor) {
                return (Compressor) result;
            } else {
                LOG.error("Class '{}' are not instance of '{}', data will be written without compression",
                        clazz.getName(), Compressor.class.getName());

                return new DummyCompressor(context);
            }
        } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InstantiationException | SecurityException ex) {

            LOG.error("Class '{}' should have public constructor with single argument of '{}'",
                    clazz.getName(), Context.class.getName());

        } catch (InvocationTargetException ex) {
            LOG.error("Unexpected exception in constructor of class '" + clazz.getName() + "'", ex);
        }

        LOG.error("Unable to instantinate compressor '{}',"
                + " data will be written without compression", clazz.getName());

        return new DummyCompressor(context);
    }   
}
