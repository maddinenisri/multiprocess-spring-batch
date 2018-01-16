package com.mdstech.batch.multiprocess;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jndi.JndiObjectFactoryBean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.Properties;

@Slf4j
@Configuration
@EnableBatchProcessing
//@EnableJpaRepositories(basePackages = {"com.mdstech.batch.multiprocess"})
@ComponentScan(basePackages = {"com.mdstech.batch.multiprocess"})
public class MultiprocessJobConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private CustomerWriter customerWriter;

    @Value("file:/Users/srini/IdeaProjects/java8-file-handler/target/data_*.csv")
    private Resource[] resources;

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource());
        em.setPackagesToScan(new String[] { "com.mdstech.batch.multiprocess" });

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(false);
        vendorAdapter.setDatabase(Database.MYSQL);
        vendorAdapter.setGenerateDdl(true);
        em.setJpaVendorAdapter(vendorAdapter);
        em.setJpaProperties(additionalProperties());
        return em;
    }

    @Bean
    public DataSource dataSource() {
        JndiObjectFactoryBean jndiObjectFactoryBean = new JndiObjectFactoryBean();
        jndiObjectFactoryBean.setJndiName("jdbc/test-pool");
        return (DataSource) jndiObjectFactoryBean.getObject();
    }

    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        JpaTransactionManager platformTransactionManager = new JpaTransactionManager();
        platformTransactionManager.setEntityManagerFactory(emf);
        return platformTransactionManager;
    }

    private Properties additionalProperties() {
        Properties properties = new Properties();
        properties.setProperty("hibernate.hbm2ddl.auto", "update");
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");
        return properties;
    }

    @Bean(name="partitionerJob")
    public Job partitionerJob() {
        return jobBuilderFactory.get("partitionerJob")
               .incrementer(new RunIdIncrementer())
               .listener(jobExecutionListener())
               .flow(partitionStep()).end().build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobCompletionNotificationListener();
    }

//    @Bean
//    public StepScope stepScope() {
//        StepScope stepScope = new StepScope();
//        stepScope.setAutoProxy(true);
//        return stepScope;
//    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public Partitioner partitioner() {
        CustomMultiResourcePartitioner customMultiResourcePartitioner = new CustomMultiResourcePartitioner();
        customMultiResourcePartitioner.setResources(resources);
        return customMultiResourcePartitioner;
    }

    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
                .<CustomerDomain, CustomerDomain>chunk(5000)
                .reader(reader(null))
                .writer(customerWriter)
                .listener(stepExecutionListener())
                .listener(chunkListener())
                .build();
    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new CustomStepListener();
    }

    @Bean
    public ChunkListener chunkListener() {
        return new CustomChunkListener();
    }

    @Bean
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
                .partitioner(slaveStep())
                .partitioner("slaveStep", partitioner())
                .taskExecutor(taskExecutor())
//                .partitionHandler(partitionHandler())
                .build();
    }

//    @Bean
//    public PartitionHandler partitionHandler() {
//        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
//        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
//        taskExecutorPartitionHandler.setGridSize(5000);
//        return taskExecutorPartitionHandler;
//    }

    @Bean
    @StepScope
    public FlatFileItemReader<CustomerDomain> reader(@Value("#{stepExecutionContext[fileName]}") String fileName) {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames("customerId,name,houseNo,streetName,state,zipCode".split(","));
        delimitedLineTokenizer.setDelimiter("|");
        DefaultLineMapper<CustomerDomain> defaultLineMapper = new DefaultLineMapper<>();
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(new RecordFiledSetMapper());
        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setLineMapper(defaultLineMapper);
        flatFileItemReader.setResource(new FileSystemResource("/Users/srini/IdeaProjects/java8-file-handler/target/"+fileName));
        return flatFileItemReader;
    }

    @Bean
    @Qualifier(value = "entityManager")
    public EntityManager entityManager(EntityManagerFactory entityManagerFactory) {
        return entityManagerFactory.createEntityManager();
    }
}
