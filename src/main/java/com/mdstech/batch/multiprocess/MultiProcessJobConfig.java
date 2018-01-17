package com.mdstech.batch.multiprocess;

import com.mdstech.batch.common.config.InfrastructureConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

@Slf4j
@Configuration
public class MultiProcessJobConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private CustomerWriter customerWriter;

    @Value("file:/Users/srini/IdeaProjects/java8-file-handler/target/data_*.csv")
    private Resource[] resources;

    @Autowired
    private InfrastructureConfiguration infrastructureConfiguration;

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

    @Bean
    public Partitioner partitioner() {
        CustomMultiResourcePartitioner customMultiResourcePartitioner = new CustomMultiResourcePartitioner();
        customMultiResourcePartitioner.setResources(resources);
        return customMultiResourcePartitioner;
    }

    @Bean
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
                .taskExecutor(infrastructureConfiguration.taskExecutor())
                .build();
    }

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

}
