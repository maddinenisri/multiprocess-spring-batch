package com.mdstech.batch.multithread;

import com.mdstech.batch.common.config.InfrastructureConfiguration;
import com.mdstech.batch.domain.CustomerDomain;
import com.mdstech.batch.multiprocess.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.persistence.EntityManagerFactory;

@Slf4j
@Configuration
public class MultiThreadJobConfig {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Bean
    @StepScope
    public ItemWriter<CustomerDomain> itemWriter() {
        CustomerWriter customerWriter = new CustomerWriter(entityManagerFactory);
        return customerWriter;
    }

    @Autowired
    private InfrastructureConfiguration infrastructureConfiguration;

    //TODO: Need to work on how to map when no files are exist, dynamically load files
    @Value("file:/tmp/multithreadJob/csv*")
    private Resource[] resources;

    @Value("file:/tmp/multithreadJob")
    private Resource workingDirectory;

    @Bean(name="multiThreadJob")
    public Job multiThreadJob() {
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>("flow1");
        Flow splitFlow =  flowBuilder
                .start(splitStep())
                .next(partitionStep())
//                .next(cleanupStep())
                .build();

        return jobBuilderFactory.get("multiThreadJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener())
                .start(splitFlow).end().build();
    }

    @Bean
    public Step cleanupStep() {
        return stepBuilderFactory.get("cleanupStep").tasklet(fileDeletingTasklet()).build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobCompletionNotificationListener();
    }

    @Bean
//    @StepScope
    public Step splitStep() {
        SystemCommandTasklet systemCommandTasklet = new SystemCommandTasklet();
        systemCommandTasklet.setCommand(String.format("split -a 5 -l 5000 %s %s", "/Users/srini/IdeaProjects/java8-file-handler/target/output_data.csv", "csv"));
        systemCommandTasklet.setTimeout(60000);
        systemCommandTasklet.setWorkingDirectory("/tmp/multithreadJob");
//        systemCommandTasklet.afterPropertiesSet();
        return stepBuilderFactory
                .get("splitStep")
                .tasklet(systemCommandTasklet)
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
//    @StepScope
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
                .<CustomerDomain, CustomerDomain>chunk(5000)
                .reader(multiResourceItemReader())
                .writer(itemWriter())
                .listener(chunkListener())
                .listener(stepExecutionListener())
                .taskExecutor(infrastructureConfiguration.taskExecutor())
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
    @StepScope
    public MultiResourceItemReader<CustomerDomain> multiResourceItemReader() {
        MultiResourceItemReader<CustomerDomain> resourceItemReader = new MultiResourceItemReader<CustomerDomain>();
        resourceItemReader.setResources(resources);
        resourceItemReader.setDelegate(reader());
        return resourceItemReader;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CustomerDomain> reader() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames("customerId,name,houseNo,streetName,state,zipCode".split(","));
        delimitedLineTokenizer.setDelimiter("|");
        DefaultLineMapper<CustomerDomain> defaultLineMapper = new DefaultLineMapper<>();
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(new RecordFiledSetMapper());
        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setLineMapper(defaultLineMapper);
//        flatFileItemReader.setResource(new FileSystemResource("/Users/srini/IdeaProjects/java8-file-handler/target/"+fileName));
        return flatFileItemReader;
    }

    @Bean
    public FlowDecision decision(){
        return new FlowDecision();
    }

    @Bean
    public FileDeletingTasklet fileDeletingTasklet() {
        FileDeletingTasklet tasklet = new FileDeletingTasklet();
        tasklet.setDirectory(workingDirectory);
        return tasklet;
    }


}
