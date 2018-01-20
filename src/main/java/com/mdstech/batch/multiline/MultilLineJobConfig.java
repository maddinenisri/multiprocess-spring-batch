package com.mdstech.batch.multiline;

import com.mdstech.batch.common.config.InfrastructureConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class MultilLineJobConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    @Bean(name="multilineJob")
    public Job multilineJob(Step step) {
        return jobBuilderFactory.get("multilineJob")
                .incrementer(new RunIdIncrementer())
                .flow(step).end().build();
    }

    @Bean
    public Step multilineStep() {
        return stepBuilderFactory.get("multilineStep")
                .<ContainerVO, ContainerVO> chunk(5)
                .reader(readerDelegate())
                .writer(itemWriter())
                .listener(itemReaderListener())
                .listener(itemWriteListener())
                .listener(chunkListener())
//                .listener(chunkListener())
                .build();
    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new MultiLineStepListener();
    }

    @Bean
    public ChunkListener chunkListener() {
        return new MultiLineChunkListiner();
    }

    @Bean
    public ItemReadListener<ContainerVO> itemReaderListener() {
        return new MultilineItemReaderListener();
    }

    @Bean
    public ItemWriteListener<ContainerVO> itemWriteListener() {
        return new MultilineItemWriterListener();
    }


    @Bean
    public ItemReader readerDelegate() {
        MultiLineItemReader multiLineItemReader = new MultiLineItemReader();
        multiLineItemReader.setDelegate(reader(null));
        return multiLineItemReader;
    }

    @Bean
    @JobScope
    public FlatFileItemReader reader(@Value("#{jobParameters['filename']}") String filename) {
        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setResource(new FileSystemResource(filename));
        flatFileItemReader.setLineMapper(patternMatchingCompositeMapper());
        return flatFileItemReader;
    }

    public PatternMatchingCompositeLineMapper patternMatchingCompositeMapper() {
        PatternMatchingCompositeLineMapper patternMatchingCompositeLineMapper = new PatternMatchingCompositeLineMapper();
        Map<String, LineTokenizer> tokenizerMap = new HashMap<>();
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter("|");
        tokenizerMap.put("1|*", delimitedLineTokenizer);
        tokenizerMap.put("*", delimitedLineTokenizer);
        patternMatchingCompositeLineMapper.setTokenizers(tokenizerMap);
        Map<String, FieldSetMapper> fieldSetMapperMap = new HashMap<>();
        fieldSetMapperMap.put("1|*", new MultilineFieldSetMapper());
        fieldSetMapperMap.put("*", new MultilineFieldSetMapper());

        patternMatchingCompositeLineMapper.setFieldSetMappers(fieldSetMapperMap);
        return patternMatchingCompositeLineMapper;
    }

    @Bean
    public ItemWriter itemWriter() {
        ConsoleItemWriter consoleItemWriter = new ConsoleItemWriter();
        return consoleItemWriter;
    }
}
