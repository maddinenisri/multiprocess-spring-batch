package com.mdstech.batch.multiprocess;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MultiprocessJobConfig.class, TestMultiprocessJobConfig.class})
public class MultiFlatfileToDBJobTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("partitionerJob")
    private Job job;

    @Autowired
    private EntityManager entityManager;

    @Test
    public void testSampleJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        JobExecution jobExecution = jobLauncher.run(job, new JobParameters());
        System.out.println(jobExecution.getStatus());
        System.out.println("Completed");

        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        countQuery.select(criteriaBuilder.count(countQuery.from(CustomerDomain.class)));
        Long count = entityManager.createQuery(countQuery) .getSingleResult();

        assertThat("Expected nearly 2.5 million", count, equalTo(2498925L));
    }
}
