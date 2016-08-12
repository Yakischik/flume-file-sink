package ru.flume.sink.counter;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class FileSinkCounter extends MonitoredCounterGroup implements FileSinkCounterMBean {

    public FileSinkCounter(String name) {
        super(Type.SINK, name, ATTRIBUTES);
    }

    // Успешные транзакции
    private static final String COUNTER_TRANSACTION_SUCCEED = "sink.transaction.succeed";
    // Пустые транзакции - попытки чтения из пустого канала
    private static final String COUNTER_TRANSACTION_EMPTY = "sink.transaction.empty";
    // Неудачные транзакции
    private static final String COUNTER_TRANSACTION_FAILED = "sink.transaction.failed";
    // Созданные файлы
    private static final String COUNTER_FILES_CREATED = "sink.file.creation.count";
    // Закрытые (сформированные) файлы
    private static final String COUNTER_FILES_CLOSED = "sink.file.closed.count";
    // Файлы, которые не получилось открыть на запись
    private static final String COUNTER_FILES_FAILED = "sink.file.failed.count";
    // Число событий, для которых была предпринята попытка записи в файлы
    private static final String COUNTER_EVENT_DRAIN_ATTEMPT = "sink.event.drain.attempt";
    // Число успешно записанных событий
    private static final String COUNTER_EVENT_DRAIN_SUCCESS = "sink.event.drain.sucess";
    // Число записанных байт
    private static final String COUNTER_EVENT_DRAIN_BYTES = "sink.event.drain.bytes";

    private static final String[] ATTRIBUTES = {
        COUNTER_TRANSACTION_SUCCEED,
        COUNTER_TRANSACTION_EMPTY,
        COUNTER_TRANSACTION_FAILED,
        COUNTER_FILES_CREATED,
        COUNTER_FILES_CLOSED,
        COUNTER_FILES_FAILED,
        COUNTER_EVENT_DRAIN_ATTEMPT,
        COUNTER_EVENT_DRAIN_SUCCESS,
        COUNTER_EVENT_DRAIN_BYTES
    };

    public long incTransactionSucceed() {
        return increment(COUNTER_TRANSACTION_SUCCEED);
    }

    public long incTransactionEmpty() {
        return increment(COUNTER_TRANSACTION_EMPTY);
    }

    public long incTransactionFailed() {
        return increment(COUNTER_TRANSACTION_FAILED);
    }

    @Override
    public long getTransactionSucceed() {
        return get(COUNTER_TRANSACTION_SUCCEED);
    }

    @Override
    public long getTransactionEmpty() {
        return get(COUNTER_TRANSACTION_EMPTY);
    }

    @Override
    public long getTransactionFailed() {
        return get(COUNTER_TRANSACTION_FAILED);
    }    
    
    public long incFilesCreated() {
        return increment(COUNTER_FILES_CREATED);
    }

    public long incFilesClosed() {
        return increment(COUNTER_FILES_CLOSED);
    }

    public long intFilesFailed() {
        return increment(COUNTER_FILES_FAILED);
    }
    
    @Override
    public long getFilesCreated() {
        return get(COUNTER_FILES_CREATED);
    }

    @Override
    public long getFilesClosed() {
        return get(COUNTER_FILES_CLOSED);
    }

    @Override
    public long getFilesFailed() {
        return get(COUNTER_FILES_FAILED);
    }

    public long addEventDrainAttemps(long delta) {
        return addAndGet(COUNTER_EVENT_DRAIN_ATTEMPT, delta);
    }

    public long addEventDrainSucceed(long delta) {
        return addAndGet(COUNTER_EVENT_DRAIN_SUCCESS, delta);
    }

    public long addBytesDrainSucceed(long delta) {
        return addAndGet(COUNTER_EVENT_DRAIN_BYTES, delta);
    }   
    
    @Override
    public long getEventDrainAttemps() {
        return get(COUNTER_EVENT_DRAIN_ATTEMPT);
    }

    @Override
    public long getEventDrainSucceed() {
        return get(COUNTER_EVENT_DRAIN_SUCCESS);
    }

    @Override
    public long getBytesDrainSucceed() {
        return get(COUNTER_EVENT_DRAIN_BYTES);
    }    
}
