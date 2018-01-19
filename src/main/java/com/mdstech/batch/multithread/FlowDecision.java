package com.mdstech.batch.multithread;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.stereotype.Component;

import java.util.Random;

@Slf4j
@Component
public class FlowDecision implements JobExecutionDecider {

    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    public static final String CONTINUE = "CONTINUE";


    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (stepExecution.getWriteCount() > 0) {
            return new FlowExecutionStatus("NEXT");
        }
        return FlowExecutionStatus.COMPLETED;
    }
}
