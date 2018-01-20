package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;

@Slf4j
public class MultilineItemReaderListener implements ItemReadListener<ContainerVO> {

    @Override
    public void beforeRead()  {
        log.info("Before Read of Item");
    }

    @Override
    public void afterRead(ContainerVO o) {
        log.info("After Read of Item, "+ o);
    }

    @Override
    public void onReadError(Exception e) {
        log.error("On Read error");
    }
}
