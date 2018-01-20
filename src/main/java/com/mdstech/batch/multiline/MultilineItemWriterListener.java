package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;

import java.util.List;

@Slf4j
public class MultilineItemWriterListener implements ItemWriteListener<ContainerVO> {
    @Override
    public void beforeWrite(List<? extends ContainerVO> list) {
        log.debug("Before Write of Item" + list);
    }

    @Override
    public void afterWrite(List<? extends ContainerVO> list) {
        log.debug("After Write of Item" + list);
    }

    @Override
    public void onWriteError(Exception e, List<? extends ContainerVO> list) {
        log.error("On Read error for writer" );
    }
}
