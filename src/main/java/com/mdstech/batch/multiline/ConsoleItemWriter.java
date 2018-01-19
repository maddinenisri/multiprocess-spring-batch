package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

@Slf4j
public class ConsoleItemWriter<T> implements ItemWriter<T> {

    @Override
    public void write(List<? extends T> items) throws Exception {
        log.info("Console item writer starts");
        for (T item : items) {
            log.info(String.format("%s", item));
        }
        log.info("Console item writer ends");

    }
}
