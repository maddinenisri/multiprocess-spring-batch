package com.mdstech.batch.common.config;

import com.mdstech.batch.multiline.MultilLineJobConfig;
import com.mdstech.batch.multiprocess.MultiProcessJobConfig;
import com.mdstech.batch.multithread.MultiThreadJobConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.ApplicationContextFactory;
import org.springframework.batch.core.configuration.support.GenericApplicationContextFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

@Slf4j
@Configuration
@EnableBatchProcessing(modular = true)
@ComponentScan(basePackages = {"com.mdstech.batch.domain", "com.mdstech.batch.common.config"})
public class ApplicationConfiguration implements BatchConfigurer {

    @Autowired
    InfrastructureConfiguration infrastructureConfiguration;

    @Bean
    @Qualifier("transactionManager")
    public PlatformTransactionManager transactionManager() {
        return infrastructureConfiguration.transactionManager();
    }

    @Bean
    public ApplicationContextFactory multiprocessJobs() {
        return new GenericApplicationContextFactory(MultiProcessJobConfig.class);
    }

    @Bean
    public ApplicationContextFactory multiThreadJobs() {
        return new GenericApplicationContextFactory(MultiThreadJobConfig.class);
    }

    @Bean
    public ApplicationContextFactory multilLineJobs() {
        return new GenericApplicationContextFactory(MultilLineJobConfig.class);
    }


    @Bean
    @Qualifier(value = "entityManager")
    public EntityManager entityManager(EntityManagerFactory entityManagerFactory) {
        return entityManagerFactory.createEntityManager();
    }

    @Override
    public JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(infrastructureConfiguration.dataSource());
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return  (JobRepository) factory.getObject();
    }

    @Override
    public PlatformTransactionManager getTransactionManager() throws Exception {
        return infrastructureConfiguration.transactionManager();
    }

    @Override
    public JobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() throws Exception {
        JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
        jobExplorerFactoryBean.setDataSource(infrastructureConfiguration.dataSource());
        jobExplorerFactoryBean.afterPropertiesSet();
        return jobExplorerFactoryBean.getObject();
    }
}
