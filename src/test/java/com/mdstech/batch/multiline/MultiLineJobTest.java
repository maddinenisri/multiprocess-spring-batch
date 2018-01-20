package com.mdstech.batch.multiline;

import com.google.common.base.Stopwatch;
import com.mdstech.batch.SpringUnitTestCaseHelper;
import com.mdstech.batch.common.config.ApplicationConfiguration;
import com.mdstech.batch.common.config.StandaloneInfrastructureConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

@Slf4j
public class MultiLineJobTest extends SpringUnitTestCaseHelper {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Test
    public void testSampleJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, NoSuchJobException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("filename", "src/main/resources/input/multiline.txt");
        Job job = jobRegistry.getJob("multilineJob");
        JobExecution jobExecution = jobLauncher.run(job, jobParametersBuilder.toJobParameters());
        System.out.println(jobExecution.getStatus());
        System.out.println("Completed");
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("time: " + stopwatch);
        System.out.println("Time it took:" + stopwatch);
    }
}
