package com.mdstech.batch.multiline;

import com.mdstech.batch.common.config.InfrastructureConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
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

    @Autowired
    private InfrastructureConfiguration infrastructureConfiguration;

    @Bean(name="multilineJob")
    public Job multilineJob() {
        return jobBuilderFactory.get("multilineJob")
                .incrementer(new RunIdIncrementer())
                .flow(multilineStep()).end().build();
    }

    @Bean
    public Step multilineStep() {
        return stepBuilderFactory.get("multilineStep")
                .<ContainerVO, ContainerVO> chunk(5)
                .reader(reader())
                .writer(writer())
                .build();
    }

    public ItemReader reader() {
        MultiLineItemReader multiLineItemReader = new MultiLineItemReader();
        multiLineItemReader.setDelegate(readerDelegate());
        return multiLineItemReader;
    }

    @Bean
    public FlatFileItemReader readerDelegate() {
        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setResource(new FileSystemResource("src/main/resources/input/multiline.txt"));
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
    public ItemWriter writer() {
        ConsoleItemWriter consoleItemWriter = new ConsoleItemWriter();
        return consoleItemWriter;
    }
}
