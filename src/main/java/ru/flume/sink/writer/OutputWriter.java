package ru.flume.sink.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.flume.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.flume.sink.compressor.Compressor;

/**
 * Класс для записи данных в файл. Непотокобезопасный, синхронизацию, если необходимо, нужно регулировать извне.
 * На время записи создается временный файл с расширением .tmp, при закрытии файла происходит его переименование.
 * Конечныое расширение файла зависит от выбранного алгоритма сжатия.
 */
public class OutputWriter {

    private static final Logger LOG = LoggerFactory.getLogger(OutputWriter.class);

    private final OutputFile file;
    private final byte[] eventSeparator;    

    private Compressor compressor;
    private OutputStream stream;
    private EventWriter eventWriter;    
    
    private volatile long lastWriteTime; 

//----------------------------------------//
    /**
     * Создает писатель для указанного файла. НЕ создает/открывает файл автоматически. Непосредственное обращение к
     * файлу и открытие его для записи происходит при записи первой порции данных.
     */
    public OutputWriter(Compressor compressor, OutputFile file, String eventSeparator) {
        this.file = file;
        this.compressor = compressor;
        this.lastWriteTime = System.currentTimeMillis();
        
        // если делителя между событиями нет, то данные будут писаться в файл "как есть" - все байты подряд
        if (eventSeparator != null && !eventSeparator.isEmpty()) {
            this.eventSeparator = eventSeparator.getBytes();            
            this.eventWriter = this::writeFirst;
        } else {
            this.eventSeparator = null;
            this.eventWriter = this::writeData;
        }   
    }
    
//----------------------------------------//
    /**
     * Создает папки и файл (если они не существуют) и открывает файл для записи.
     * 
     * @throws IOException  если невозможно получить доступ к файлу
     */
    void init() throws IOException {
        if (!file.getParent().exists()) {
            file.getParent().mkdirs();
        }
        stream = compressor.wrap(new FileOutputStream(file.getTemp()));
    }
    
//----------------------------------------//
    /**
     * Записывает событие флюма в файл, обновляет время последнего обращения к этому файлу. Метод непотокобезопасный.
     */
    public void write(Event event) throws IOException {
        if (event.getBody().length > 0) {
            lastWriteTime = System.currentTimeMillis();
            eventWriter.write(event.getBody());
        }
    }
    
    // EventWriter и вся эта чехарда с лямбдами используется только для того, чтобы не заканчивать файл пустой строкой
    // и не проверять каждый раз какой-нибудь флаг типа "isFirstLine".
    private void writeData(byte[] data) throws IOException  {
        stream.write(data);
    }
    
    private void writeFirst(byte[] data) throws IOException {
        stream.write(data);
        eventWriter = this::writeNext;
    }
    
    private void writeNext(byte[] data) throws IOException {
        stream.write(eventSeparator);
        stream.write(data);
    }

//----------------------------------------//
    /**
     * Делегат аналогичного метода от потока записи.
     */
    public synchronized void flush() throws IOException{
        stream.flush();
    }
    
//----------------------------------------//
    /**
     * Закрывает файл, записывая остатки буфера в него. После этого переименовывает файл в конечный вид.
     */
    public void close() {
        File output = file.getOutput(compressor.getExtension());
        File temp = file.getTemp();
        if (stream != null) {
            try {
                stream.flush();
                stream.close();
            } catch (IOException ex) {
                LOG.error("Exception while close stream for file: " + output, ex);
            }
        }        
        temp.renameTo(output);
    }
    
//----------------------------------------//
    /**
     * @return время последней записи в файл
     */
    public long getLastWriteTime() {
        return lastWriteTime;
    }

//----------------------------------------//
    /**
     * @return полное имя файла
     */
    public String getFileName() {
        return file.getName();
    }
     
//****************************************//  

    private static interface EventWriter {

        public void write(byte[] data) throws IOException;
    }
}
