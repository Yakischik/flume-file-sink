package ru.flume.sink.counter;

public interface FileSinkCounterMBean {

    long getBytesDrainSucceed();

    long getEventDrainAttemps();

    long getEventDrainSucceed();

    long getFilesClosed();

    long getFilesCreated();

    long getFilesFailed();

    long getTransactionEmpty();

    long getTransactionFailed();

    long getTransactionSucceed();
    
    long getStartTime();

    long getStopTime();

    String getType();
}
