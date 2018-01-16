package com.mdstech.batch.multiprocess;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.util.Arrays;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
public class CustomChunkListener implements ChunkListener {

    @Override
    public void beforeChunk(ChunkContext context) {
        log.info("ChunkListener ---- before chunk called" + Arrays.stream(context.attributeNames()).map(s -> s).collect(Collectors.joining(",")));
    }

    @Override
    public void afterChunk(ChunkContext context) {
        log.info("ChunkListener ---- after chunk called");
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        log.info("ChunkListener ---- after chunk error called");
    }
}
