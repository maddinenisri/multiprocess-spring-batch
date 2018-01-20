package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

@Slf4j
public class MultiLineChunkListiner implements ChunkListener {

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        log.info("Before Chunk");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        log.info("After Chunk");
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        log.info("Got ERROR for chunk");
    }
}
