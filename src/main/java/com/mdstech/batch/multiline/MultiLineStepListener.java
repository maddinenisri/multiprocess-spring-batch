package com.mdstech.batch.multiline;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

@Slf4j
public class MultiLineStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("Before Step execution");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("After Step Execution");
        return null;
    }
}
