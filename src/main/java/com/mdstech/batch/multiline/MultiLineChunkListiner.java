package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

@Slf4j
public class MultiLineChunkListiner implements ChunkListener {

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        log.debug("Before Chunk");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        log.debug("After Chunk");
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        log.debug("Got ERROR for chunk");
    }
}
