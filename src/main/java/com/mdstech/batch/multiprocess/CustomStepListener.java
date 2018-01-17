package com.mdstech.batch.multiprocess;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

@Slf4j
public class CustomStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("StepExecutionListener  ---- before  ");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("StepExecutionListener  ---- after  ");
        return null;
    }
}
