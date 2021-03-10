package ru.volkov.batch.output.multifile;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import ru.volkov.batch.output.domain.Customer;
import ru.volkov.batch.output.domain.CustomerLineAggregator;
import ru.volkov.batch.output.domain.CustomerRowMapper;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class MultiFileJobConfiguration {

    private StepBuilderFactory stepBuilderFactory;
    private JobBuilderFactory jobBuilderFactory;
    private DataSource dataSource;

    public MultiFileJobConfiguration(
            StepBuilderFactory stepBuilderFactory,
            JobBuilderFactory jobBuilderFactory,
            DataSource dataSource) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public JdbcPagingItemReader<Customer> jdbcPagingItemReader() {
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(this.dataSource);
        reader.setRowMapper(new CustomerRowMapper());

        PostgresPagingQueryProvider provider = new PostgresPagingQueryProvider();
        provider.setSelectClause("id, name, date");
        provider.setFromClause("output.customers");

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);

        provider.setSortKeys(sortKeys);

        reader.setQueryProvider(provider);

        return reader;
    }

    @Bean
    public FlatFileItemWriter<Customer> jsonItemWriter() throws Exception {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();

        writer.setLineAggregator(new CustomerLineAggregator());

        String outputPath = File.createTempFile("jsonCustomers", ".out").getAbsolutePath();
        System.out.println(">>> Output path -'{" + outputPath + "'}");
        writer.setResource(new FileSystemResource(outputPath));
        writer.afterPropertiesSet();

        return writer;
    }

    @Bean
    public StaxEventItemWriter<Customer> xmlItemWriter() throws Exception {

        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("customer", Customer.class);
        marshaller.setAliases(aliases);

        StaxEventItemWriter<Customer> writer = new StaxEventItemWriter<>();
        writer.setRootTagName("customers");
        writer.setMarshaller(marshaller);
        String outputPath = File.createTempFile("xmlCustomers", ".xml").getAbsolutePath();
        System.out.println(">>> Output path -'{" + outputPath + "'}");
        writer.setResource(new FileSystemResource(outputPath));

        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    public CompositeItemWriter<Customer> compositeItemWriter() throws Exception {
        List<ItemWriter<? super Customer>> writers = new ArrayList<>(2);

        writers.add(xmlItemWriter());
        writers.add(jsonItemWriter());

        CompositeItemWriter<Customer> compositeWriter = new CompositeItemWriter<>();
        compositeWriter.setDelegates(writers);
        compositeWriter.afterPropertiesSet();

        return compositeWriter;
    }

    @Bean
    public Step compositeStep() throws Exception {
        return stepBuilderFactory.get("compositeStep")
                .<Customer, Customer>chunk(10)
                .reader(jdbcPagingItemReader())
                .writer(compositeItemWriter())
                .build();

    }

    @Bean
    public Job compositeJob() throws Exception {
        return jobBuilderFactory.get("compositeJob")
                .start(compositeStep())
                .build();
    }
}
