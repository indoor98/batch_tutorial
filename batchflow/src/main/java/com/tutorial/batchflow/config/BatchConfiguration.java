package com.tutorial.batchflow.config;

import com.tutorial.batchflow.decider.MyJobExecutionDecider;
import com.tutorial.batchflow.domain.*;
import com.tutorial.batchflow.processor.FilterProductItemProcessor;
import com.tutorial.batchflow.processor.TransformProductItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class BatchConfiguration {

    @Autowired
    public DataSource dataSource;


    @Bean
    public ItemProcessor<Product, OSProduct> transformProductItemProcessor() {
        return new TransformProductItemProcessor();
    }

    @Bean
    public ItemProcessor<Product, Product> filterProductItemProcessor() {
        return new FilterProductItemProcessor();
    }

    @Bean
    public ValidatingItemProcessor<Product> validatingItemProcessor() {
        ValidatingItemProcessor<Product> validatingItemProcessor = new ValidatingItemProcessor<>( new ProductValidator() );
        validatingItemProcessor.setFilter(true); // Job may not fail even if not valid
        return validatingItemProcessor;
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
    public ItemReader<Product> jdbcCursorItemReader() {
        JdbcCursorItemReader<Product> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("SELECT * FROM product_details ORDER BY product_id"); // order by 안쓰면 순서 무작위임
        itemReader.setRowMapper(new ProductRowMapper() );
        return itemReader;
    }



    @Bean
    public ItemReader<Product> jdbcPagingItemReader() throws Exception {
        JdbcPagingItemReader<Product> itemReader = new JdbcPagingItemReader<>();
        itemReader.setDataSource(dataSource);

        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setDataSource(dataSource);
        factory.setSelectClause("SELECT product_id, product_name, product_category, product_price");
        factory.setFromClause("FROM product_details");
        factory.setSortKey("product_id"); // SortKey Should be unique

        itemReader.setQueryProvider(factory.getObject());
        itemReader.setRowMapper(new ProductRowMapper());
        itemReader.setPageSize(3); //Pagesize == chunk size하는 것을 권장

        return itemReader;
    }
    @Bean
    public ItemWriter<Product> flatFIleItemWriter() {
        FlatFileItemWriter<Product> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource("batchchunk/src/main/resources/data/Product_Details_Output.csv"));

        DelimitedLineAggregator<Product> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(","); // 구분자

        BeanWrapperFieldExtractor<Product> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"productId", "productName", "productCategory", "productPrice"});

        lineAggregator.setFieldExtractor(fieldExtractor);

        itemWriter.setLineAggregator(lineAggregator);
        return itemWriter;
    }

//    @Bean
//    public JdbcBatchItemWriter<Product> jdbcBatchItemWriter() {
//        JdbcBatchItemWriter<Product> itemWriter = new JdbcBatchItemWriter<>();
//        itemWriter.setDataSource(dataSource);
//        itemWriter.setSql("INSERT INTO product_details_output VALUES(:productId, :productName, :productCategory, :productPrice)");
//        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
//
//        return itemWriter;
//    }

    @Bean
    public JdbcBatchItemWriter<OSProduct> jdbcBatchItemWriter() {
        JdbcBatchItemWriter<OSProduct> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("INSERT INTO os_product_details VALUES(:productId, :productName, :productCategory, :productPrice, :taxPercent, :sku, :shippingRate)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());

        return itemWriter;
    }

    @Bean
    public BeanValidatingItemProcessor<Product> validateProductItemProccessor() {
        BeanValidatingItemProcessor<Product> beanValidatingItemProcessor = new BeanValidatingItemProcessor<>();
        beanValidatingItemProcessor.setFilter(true);
        return beanValidatingItemProcessor;
    }

    @Bean
    public CompositeItemProcessor<Product, OSProduct> itemProcessor() {
        CompositeItemProcessor<Product, OSProduct> itemProcessor = new CompositeItemProcessor<>();
        List itemProcessors = new ArrayList();
        itemProcessors.add(validateProductItemProccessor());
        itemProcessors.add(filterProductItemProcessor());
        itemProcessors.add(transformProductItemProcessor());
        itemProcessor.setDelegates(itemProcessors);
        return itemProcessor;
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
        return new StepBuilder("chunkBasedStep1", jobRepository)
                .<Product, OSProduct>chunk(3, transactionManager)
                .reader(jdbcPagingItemReader())
                .processor(itemProcessor())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    @Bean
    public Job firstJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
        return new JobBuilder("job1", jobRepository)
                .start(step1(jobRepository, transactionManager))
                .build();
    }

}