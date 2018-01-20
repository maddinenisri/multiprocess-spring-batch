package com.mdstech.batch.multithread;

import com.google.common.base.Stopwatch;
import com.mdstech.batch.SpringUnitTestCaseHelper;
import com.mdstech.batch.common.config.ApplicationConfiguration;
import com.mdstech.batch.domain.CustomerDomain;
import com.mdstech.batch.common.config.StandaloneInfrastructureConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.transaction.TransactionManager;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@Slf4j
public class MultiThreadFlatfileToDBJobTest extends SpringUnitTestCaseHelper {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    @Qualifier("transactionManager")
    private PlatformTransactionManager transactionManager;

    @Before
    public void setUp() throws Exception {
        TransactionDefinition def = new DefaultTransactionDefinition();
        TransactionStatus status = transactionManager.getTransaction(def);
        entityManager.joinTransaction();
        entityManager.createQuery("DELETE FROM CustomerDomain e").executeUpdate();
        transactionManager.commit(status);
    }

    private JobParameters getJobParameters() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("inputFile", "/Users/srini/IdeaProjects/java8-file-handler/target/output_data.csv");
        jobParametersBuilder.addString("stagingDirectory", "/tmp/multithreadJob");
        jobParametersBuilder.addString("iputFilesPath", "file:/tmp/multithreadJob/csv*");
        jobParametersBuilder.addString("workingDirectory", "file:/tmp/multithreadJob");
        return jobParametersBuilder.toJobParameters();
    }

    @Test
    public void testSampleJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, NoSuchJobException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Job job = jobRegistry.getJob("multiThreadJob");
        JobExecution jobExecution = jobLauncher.run(job, getJobParameters());
        System.out.println(jobExecution.getStatus());
        System.out.println("Completed");
        stopwatch.stop();
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("time: " + stopwatch);
        System.out.println("Time it took:" + stopwatch);
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        countQuery.select(criteriaBuilder.count(countQuery.from(CustomerDomain.class)));
        Long count = entityManager.createQuery(countQuery) .getSingleResult();

        assertThat("Expected nearly 2.5 million", count, equalTo(2498925L));
    }

}

