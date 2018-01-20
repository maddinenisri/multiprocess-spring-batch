package com.mdstech.batch.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

public interface InfrastructureConfiguration {

    @Bean
    public abstract DataSource dataSource();

    @Bean
    public abstract TaskExecutor taskExecutor();

    @Bean
    public abstract PlatformTransactionManager transactionManager();
}
