package ru.flume.sink.writer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.flume.sink.compressor.Compressor;
import ru.flume.sink.counter.FileSinkCounter;

/**
 * "Центр управления" записью в файлы. Предоставляет доступ к файлам для записи и периодически проверяет - давно ли
 * поступали данные в файлы. Если к файлу долго не было обращений, то закрывает его.
 */
public class FileManager {    
    
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    private static final String PARAM_DIRECTORY = "sink.dir";

    private static final String PARAM_SEPARATOR = "sink.eventSeparator";
    private static final String DEFAULT_SEPARATOR = "\n";

    private static final String PARAM_IDLE_TIMEOUT = "sink.idleTimeout";
    private static final long DEFAULT_IDLE_TIMEOUT = 60 * 60 * 1000;

    private static final String PARAM_FLUSH_TIMEOUT = "sink.flushTimeout";
    private static final long DEFAULT_FLUSH_TIMEOUT = 1 * 60 * 1000;

    private static final String PARAM_CHECK_PERIOD = "sink.checkPeriod";
    private static final long DEFAULT_CHECK_PERIOD = 1 * 60 * 1000;

    private Map<String, OutputWriter> writers = new HashMap<>(128);
    private ScheduledExecutorService idleChecker = Executors.newScheduledThreadPool(1);

    private final File directory;
    private final Compressor compressor;
    private final String eventSeparator;
    private final long idleTimeout;
    private final long flushTimeout;
    
    private final FileSinkCounter counter;
    
//----------------------------------------//
    /**
     * Создает менеджер, который будет создавать файлы в указанной папке и предоставлять средства записи для этих
     * файлов. Автоматически запускает фоновый поток для проверки неактивности отдельных файлов.
     *
     * @param directory     корневой каталог для файлов
     * @param idleTimeout   время неактивности, после которого следует закрывать файл
     * @param checkPeriod   периодичность проверки неактивных файлов
     */
    public FileManager(Context context, FileSinkCounter counter) {
        this.counter = counter;
        
        String rootDirectory = context.getString(PARAM_DIRECTORY, null);
        if (StringUtils.isBlank(rootDirectory)) { 
            throw new IllegalArgumentException("Directory for output files not specified");
        }
        
        this.directory = new File(rootDirectory);
        this.compressor = Compressor.createCompressor(context);
        
        this.eventSeparator = context.getString(PARAM_SEPARATOR, DEFAULT_SEPARATOR);
        this.idleTimeout =  context.getLong(PARAM_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
        this.flushTimeout =  context.getLong(PARAM_FLUSH_TIMEOUT, DEFAULT_FLUSH_TIMEOUT);
        long checkPeriod = context.getLong(PARAM_CHECK_PERIOD, DEFAULT_CHECK_PERIOD);        
        
        if (idleTimeout <= 0) {
            throw new IllegalArgumentException("Idle timeout should be greater than 0");
        }        
        if (flushTimeout <= 0) {
            throw new IllegalArgumentException("Flush timeout should be greater than 0");
        }        
        if (checkPeriod <= 0) {
            throw new IllegalArgumentException("Check period should be greater than 0");
        }        
        
        LOG.info("Created FileManager with params:"
                + "\n\tDirectory: " + directory
                + "\n\tIdle timeout: " + idleTimeout + "ms"
                + "\n\tFlush timeout: " + flushTimeout + "ms"
                + "\n\tCheck period: " + checkPeriod + "ms"
                + "\n\tCompressor: " + compressor.getClass().getSimpleName() 
                    + " (file extension: '" + compressor.getExtension() + "')"
        );
        
        this.idleChecker.scheduleAtFixedRate(this::checkIdle, checkPeriod, checkPeriod, TimeUnit.MILLISECONDS);  
    }
    
//----------------------------------------//
    /**
     * Отдает FileWriter для указанного имени файла или создает новый. В случае, если создан новый файл и FileWriter для
     * него, FileWriter будет закеширован строго по тому имени файла, которое переданного в аргументе. Таким образом,
     * остается замечательный баг, когда один и тот же файл может породить 2 FileWriter'а (например, "/myFile" и
     * "myFile"). Чтобы этого избежать, используйте в заголовках событий пути до файла в едином формате.
     * 
     * @param fileName  имя файла, в который будет вестись запись
     */
    public OutputWriter getWriter(String fileName) throws IOException {
        OutputWriter writer = writers.get(fileName);
        if (writer == null) {
            synchronized (this) {                
                writer = writers.get(fileName);
                if (writer == null) {
                    OutputFile file = new OutputFile(directory, fileName);
                    writer = new OutputWriter(compressor, file, eventSeparator);
                    try {
                        writer.init();
                        counter.incFilesCreated();
                    } catch (Exception e) {
                        counter.intFilesFailed();
                        writer.close();
                        throw e;
                    }
                    writers.put(fileName, writer);
                    LOG.info("Writing new file: '{}' ", fileName); 
                }
            }
        }
        return writer;
    }

//----------------------------------------//
    /**
     * Метод фоновой проверки открытых файлов на предмет неактивности.     
     */
    private void checkIdle() {
        Map<String, OutputWriter> toClose = new TreeMap<>();
        Map<String, OutputWriter> toFlush = new TreeMap<>();
        
        gatherExpired(toClose, toFlush);
        
        closeWriters(toClose);
        flushWriters(toFlush);
    }
    
//----------------------------------------//
    // Метод синхронизирован для предотвращения конкурентного доступа к 'writers' Map.    
    private synchronized void gatherExpired(Map<String, OutputWriter> toClose, Map<String, OutputWriter> toFlush) {
        long now = System.currentTimeMillis();
        for (Entry<String, OutputWriter> e : writers.entrySet()) {
            String key = e.getKey();
            OutputWriter writer = e.getValue();
            long idleTime = now - e.getValue().getLastWriteTime();
            if (idleTime > idleTimeout) {
                toClose.put(key, writer);
            } else if (idleTime > flushTimeout) {
                toFlush.put(key, writer);
            }
        }
        if (!toClose.isEmpty()) {
            removeWriters(toClose.keySet());
        }
    }
    
//----------------------------------------//
    // Здесь также исключаем конкурентный доступ
    private synchronized void removeWriters(Collection<String> keys) {
        for (String key : keys) {
            writers.remove(key);
        }
    }
    
//----------------------------------------//
    // Flush выполняеется вне синхронизированного блока, чтобы излишне не блокировать доступ к 'writers' Map.
    private void flushWriters(Map<String, OutputWriter> toFlush) {
        if (!toFlush.isEmpty()) {
            Map<String, OutputWriter> toClose = new TreeMap<>();
            for (Map.Entry<String, OutputWriter> e : toFlush.entrySet()) {
                OutputWriter writer = e.getValue();
                String key = e.getKey();
                try {
                    writer.flush();
                } catch (Throwable ex) {
                    counter.intFilesFailed();
                    LOG.error("Unexpected exception during flush buffer to file: " + e.getKey(), ex);
                    toClose.put(key, writer);
                }
            }
            if (!toClose.isEmpty()) {
                // А вот здесь приодится лезть в синхронный метод, если вдруг появились файлы, которые нужно закрыть
                removeWriters(toClose.keySet());
                closeWriters(toClose);
            }
        }
    }
    
//----------------------------------------//    
    private void closeWriters(Map<String, OutputWriter> toClose) {
        if (!toClose.isEmpty()) {
            for (Map.Entry<String, OutputWriter> e : toClose.entrySet()) {
                OutputWriter writer = e.getValue();
                writer.close();
                counter.incFilesClosed();
                LOG.info("File {} closed", writer.getFileName());
            }
            LOG.info("{} idle files closed, {} files now in use", toClose.size(), writers.size());
        }
    }
    
//----------------------------------------//
    /**
     * Закрывает все открытые файлы.
     */
    public synchronized void closeAll(){
        idleChecker.shutdown();
        for (OutputWriter writer : writers.values()) {
            writer.close();
        }
        LOG.info("All {} files closed", writers.size());
        writers.clear();
    }    
}
