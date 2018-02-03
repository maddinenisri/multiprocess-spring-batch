package com.mdstech.batch.common.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.util.List;

@Slf4j
public class CustomListener implements ItemWriteListener, StepExecutionListener, ItemReadListener, ChunkListener, SkipListener {

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        log.info("beforeChunk");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        log.info("afterChunk");
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        log.info("afterChunkError");
    }

    @Override
    public void beforeRead() {
        log.info("beforeRead");
    }

    @Override
    public void onReadError(Exception e) {
        log.info("onReadError");
    }

    @Override
    public void afterRead(Object o) {
        log.info("afterRead");
    }

    @Override
    public void beforeWrite(List list) {
        log.info("beforeWrite");
    }

    @Override
    public void afterWrite(List list) {
        log.info("afterWrite");
    }

    @Override
    public void onWriteError(Exception e, List list) {
        log.info("onWriteError");
    }

    @Override
    public void onSkipInRead(Throwable throwable) {
        log.info("onSkipInRead");
    }

    @Override
    public void onSkipInWrite(Object o, Throwable throwable) {
        log.info("onSkipInWrite");
    }

    @Override
    public void onSkipInProcess(Object o, Throwable throwable) {
        log.info("onSkipInProcess");
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("beforeStep");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("afterStep");
        return null;
    }
}
