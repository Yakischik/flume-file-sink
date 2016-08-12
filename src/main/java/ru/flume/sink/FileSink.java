package ru.flume.sink;

import java.io.IOException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.flume.sink.writer.OutputWriter;
import ru.flume.sink.writer.FileManager;
import ru.flume.sink.counter.FileSinkCounter;

/**
 * Сток Flume, позволяющий фасовать взодящие событие по отдельным файлам. Имя файла, в которое будет записано событие,
 * определяется заголовком. Т.е., таким образом, клиенты сами определяют - куда и что писать, а данный сток является
 * "дирижёром", централизовано записывающим события в файлы.
 * 
 *<ul>
 *<li><code>fileNameHeader</code> - заголовок, в котором передается имя файла ('file')
 *<li><code>batchSize</code> - кол-во событий, обрабатываемых за одну транзацию (1000)
 *<li><code>eventSeparator</code> - разделитель событий ('\n')
 *<li><code>idleTimeout</code> - время неактивности файла в мс, после которого он будет закрыт (1 час)
 *<li><code>flushTimeout</code> - время неактивности файла в мс, после которого данные из буфера сбросятся в файл (1 мин.)
 *<li><code>checkPeriod</code> - как часто проверять наличие неактивный файлов (1 мин)
 *<li><code>compressor</code> - метод сжатия данных (gzip или text)
 *</ul>
 */
public class FileSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);

    private static final String PARAM_HEADER_FILE_NAME = "sink.fileNameHeader";
    private static final String DEFAULT_HEADER_FILE_NAME = "file";

    private static final String PARAM_BATCH_SIZE = "sink.batchSize";
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private int batchSize;
    private String fileNameHeader;
    private FileManager fileManager;    
    private FileSinkCounter counter;

//----------------------------------------//
    @Override
    public void configure(Context context) {
        this.fileNameHeader = context.getString(PARAM_HEADER_FILE_NAME, DEFAULT_HEADER_FILE_NAME);
        this.batchSize = context.getInteger(PARAM_BATCH_SIZE, DEFAULT_BATCH_SIZE);

        if (this.counter == null) {
            this.counter = new FileSinkCounter(getName());            
        }
        if (this.fileManager == null) {
            this.fileManager = new FileManager(context, counter);
        }
        
        LOG.info("Created File-Sink with params:"
                + "\n\tHeader (file name): " + fileNameHeader
                + "\n\tBatch size: " + batchSize);
    }

//----------------------------------------//
    @Override
    public void start() {
        LOG.info("Starting '{}' sink", getName());
        this.counter.start();
        super.start();
        LOG.info("Sink '{}' started.", getName());
    }

//----------------------------------------//    
    @Override
    public Sink.Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Sink.Status result = Sink.Status.READY;

        int attemps = 0;
        int succeed = 0;
        long bytes = 0;
        
        try {
            transaction.begin();            
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {
                    attemps++;
                    String fileName = event.getHeaders().get(fileNameHeader);                    

                    // события без интересующих наз заголовков будут пропускаться
                    if (fileName == null) {
                        continue;
                    }
                    OutputWriter writer = fileManager.getWriter(fileName);
                    try {
                        writer.write(event);
                        bytes += event.getBody().length;
                        succeed++;
                    } catch (IOException e) {                        
                        // TODO: Здесь не очень корректно с точки зрения транзакции, т.к. часть событий может быть 
                        // записана,а при откате транзакции они "вернутся в канал" - т.е., возможно дублирование данных
                        throw new EventDeliveryException("Failed to open file "
                                + fileName + " while delivering event", e);
                    } 
                } else {
                    if (attemps == 0) {
                        counter.incTransactionEmpty();
                    }
                    // В канале больше нет событий, заканчиваем запись
                    result = Sink.Status.BACKOFF;
                    break;
                }
            }
            transaction.commit();  
            if (attemps > 0) {
                counter.incTransactionSucceed();
            }
        } catch (Exception ex) {
            transaction.rollback();
            counter.incTransactionFailed();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            counter.addBytesDrainSucceed(bytes);
            counter.addEventDrainAttemps(attemps);
            counter.addEventDrainSucceed(succeed);
            transaction.close();
        }
        return result;
    }

//----------------------------------------//    
    @Override
    public void stop() {
        LOG.info("Sink '{}' trying to shutdown", getName());
        super.stop();
        fileManager.closeAll();
        LOG.info("'{}' stopped.", getName());
    }
}
