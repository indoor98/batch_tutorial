package com.tutorial.batchchunk.config;

import com.tutorial.batchchunk.decider.MyJobExecutionDecider;
import com.tutorial.batchchunk.domain.Product;
import com.tutorial.batchchunk.domain.ProductFieldSetMapper;
import com.tutorial.batchchunk.listener.MyStepExecutionListener;
import com.tutorial.batchchunk.decider.MyJobExecutionDecider;
import com.tutorial.batchchunk.reader.ProductNameItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {


    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public ItemReader<String> itemReader() {
        List<String> productList = new ArrayList<>();
        productList.add("Product1");
        productList.add("Product2");
        productList.add("Product3");
        productList.add("Product4");
        productList.add("Product5");
        productList.add("Product6");
        productList.add("Product7");
        productList.add("Product8");
        return new ProductNameItemReader(productList);
    }


    @Bean
    public MyJobExecutionDecider myStepExecutionListener() {
        return new MyJobExecutionDecider();
    }

    @Bean
    public MyJobExecutionDecider myJobExecutionDecider() {
        return new MyJobExecutionDecider();
    }


    @Bean
    public ItemReader<Product> flatFileItemReader() {
        FlatFileItemReader<Product> itemReader = new FlatFileItemReader<>();
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new ClassPathResource("/data/product_details.csv"));

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("product_id", "product_name", "product_category", "product_price");

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(new ProductFieldSetMapper());

        itemReader.setLineMapper(lineMapper);
        return itemReader;
    }

    @Bean
    public Step step1() {
        return this.stepBuilderFactory.get("chunkBasedFactory")
                .<Product, Product>chunk(3)
                .reader(flatFileItemReader())
                .writer(new ItemWriter<Product>() {
                    @Override
                    public void write(List<? extends Product> items) throws Exception {
                        System.out.println("Chunk-processing Started");
                        items.forEach(System.out::println);
                        System.out.println("Chunk-processing Ended");
                    }
                })
                .build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                boolean isFailure = false;

                if(isFailure) {
                    throw new Exception("Test Exception");
                }


                System.out.println("step2 execute!!");
                return RepeatStatus.FINISHED;
            }
        }).listener(myStepExecutionListener()).build(); // Step Listener 추가
    }

    @Bean
    public Step step3() {
        return this.stepBuilderFactory.get("step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("step3 execute!!");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step step4() {
        return this.stepBuilderFactory.get("step4").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("step4 execute!!");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Job firstJob() {
        return this.jobBuilderFactory.get("job1")
                .start(step1())
                .on("COMPLETED").to(myJobExecutionDecider())
                .on("TEST_STATUS").to(step2())// Step Exit Status
                .from(myJobExecutionDecider())
                .on("*").to(step3())
                .end()
                .build();
    }

}