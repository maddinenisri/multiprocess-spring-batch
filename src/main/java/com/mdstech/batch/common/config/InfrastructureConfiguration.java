package com.mdstech.batch.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;

public interface InfrastructureConfiguration {

    @Bean
    public abstract DataSource dataSource();

    @Bean
    public abstract TaskExecutor taskExecutor();
}
