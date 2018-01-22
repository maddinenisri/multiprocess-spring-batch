package com.mdstech.batch.multithread;

import com.mdstech.batch.common.config.InfrastructureConfiguration;
import com.mdstech.batch.domain.CustomerDomain;
import com.mdstech.batch.multiprocess.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
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

    @Autowired
    private InfrastructureConfiguration infrastructureConfiguration;

    @Bean
    @StepScope
    public ItemWriter<CustomerDomain> itemWriter() {
        CustomerWriter customerWriter = new CustomerWriter(entityManagerFactory);
        return customerWriter;
    }

    @Bean(name="multiThreadJob")
    public Job multiThreadJob() {
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>("flow1");
        Flow splitFlow =  flowBuilder
                .start(splitStep())
                .next(partitionStep())
                .next(cleanupStep())
                .build();

        return jobBuilderFactory.get("multiThreadJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener())
                .start(splitFlow).end().build();
    }

    @Bean
    public Step cleanupStep() {
        return stepBuilderFactory.get("cleanupStep").tasklet(fileDeletingTasklet(null)).build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobCompletionNotificationListener();
    }

    @Bean
    public Step splitStep() {
        return stepBuilderFactory
                .get("splitStep")
                .tasklet(systemCommandTasklet(null, null))
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
    @JobScope
    public SystemCommandTasklet systemCommandTasklet(@Value("#{jobParameters['inputFile']}") String inputFile, @Value("#{jobParameters['stagingDirectory']}") String stagingDirectory) {
        SystemCommandTasklet systemCommandTasklet = new SystemCommandTasklet();
        systemCommandTasklet.setCommand(String.format("split -a 5 -l 5000 %s %s", inputFile, "csv"));
        systemCommandTasklet.setTimeout(60000);
        systemCommandTasklet.setWorkingDirectory(stagingDirectory);
        return systemCommandTasklet;
    }

    @Bean
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
//                .partitioner(taskletStep())
                .partitioner("taskletStep", multiThreadPartitioner(null))
                .partitionHandler(partitionHandler())
//                .taskExecutor(infrastructureConfiguration.taskExecutor())
                .build();

//        return stepBuilderFactory.get("partitionStep")
//                .<CustomerDomain, CustomerDomain>chunk(5000)
//                .reader(multiResourceItemReader(null))
//                .writer(itemWriter())
//                .listener(chunkListener())
//                .listener(stepExecutionListener())
//                .taskExecutor(infrastructureConfiguration.taskExecutor())
//                .build();
    }

    @Bean
    public PartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(8);
        partitionHandler.setTaskExecutor(infrastructureConfiguration.taskExecutor());
        partitionHandler.setStep(taskletStep());
        return partitionHandler;
    }

    @Bean
    public Step taskletStep() {
        return stepBuilderFactory.get("taskletStep")
                .<CustomerDomain, CustomerDomain>chunk(5000)
                .reader(reader(null, null))
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Tasklet tasklet() {
        return new MultiThreadTasklet();
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
    public Partitioner multiThreadPartitioner(@Value("#{jobParameters['iputFilesPath']}") Resource[] resources) {
        MultiThreadPartitioner multiThreadPartitioner = new MultiThreadPartitioner();
        multiThreadPartitioner.setResources(resources);
        return multiThreadPartitioner;
    }

//    @Bean
//    @StepScope
//    public MultiResourceItemReader<CustomerDomain> multiResourceItemReader(@Value("#{jobParameters['iputFilesPath']}") Resource[] resources) {
//        MultiResourceItemReader<CustomerDomain> resourceItemReader = new MultiResourceItemReader<CustomerDomain>();
//        resourceItemReader.setResources(resources);
//        resourceItemReader.setDelegate(reader());
//        return resourceItemReader;
//    }

    @Bean
    @StepScope
    public FlatFileItemReader<CustomerDomain> reader(@Value("#{jobParameters['stagingDirectory']}") String stagingDirectory,
                                                     @Value("#{stepExecutionContext['file']}") String resource) {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames("customerId,name,houseNo,streetName,state,zipCode".split(","));
        delimitedLineTokenizer.setDelimiter("|");
        DefaultLineMapper<CustomerDomain> defaultLineMapper = new DefaultLineMapper<>();
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(new RecordFiledSetMapper());
        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setLineMapper(defaultLineMapper);
        flatFileItemReader.setResource(new FileSystemResource(stagingDirectory+"/"+resource));

        return flatFileItemReader;
    }

    @Bean
    public FlowDecision decision(){
        return new FlowDecision();
    }

    @Bean
    @JobScope
    public FileDeletingTasklet fileDeletingTasklet(@Value("#{jobParameters['workingDirectory']}") Resource workingDirectory) {
        FileDeletingTasklet tasklet = new FileDeletingTasklet();
        tasklet.setDirectory(workingDirectory);
        return tasklet;
    }
}
