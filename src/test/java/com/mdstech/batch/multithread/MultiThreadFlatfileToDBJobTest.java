package com.mdstech.batch.multithread;

import com.mdstech.batch.common.config.ApplicationConfiguration;
import com.mdstech.batch.domain.CustomerDomain;
import com.mdstech.batch.common.config.StandaloneInfrastructureConfiguration;
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

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ApplicationConfiguration.class, StandaloneInfrastructureConfiguration.class})
public class MultiThreadFlatfileToDBJobTest {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

//    @Autowired
//    @Qualifier("partitionerJob")
//    private Job job;

    @Autowired
    private EntityManager entityManager;

    private JobParameters getJobParameters() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("inputFile", "/Users/srini/IdeaProjects/java8-file-handler/target/output_data.csv");
        jobParametersBuilder.addString("stagingDirectory", "/tmp/multithreadJob");
        return jobParametersBuilder.toJobParameters();
    }

    @Test
    public void testSampleJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, NoSuchJobException {
        Job job = jobRegistry.getJob("multiThreadJob");
        JobExecution jobExecution = jobLauncher.run(job, getJobParameters());
        System.out.println(jobExecution.getStatus());
        System.out.println("Completed");

        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        countQuery.select(criteriaBuilder.count(countQuery.from(CustomerDomain.class)));
        Long count = entityManager.createQuery(countQuery) .getSingleResult();

        assertThat("Expected nearly 2.5 million", count, equalTo(2498925L));
    }

}

