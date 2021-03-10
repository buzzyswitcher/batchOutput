package ru.volkov.batch.output.xml;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import ru.volkov.batch.output.domain.Customer;
import ru.volkov.batch.output.domain.CustomerRowMapper;

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class XmlJobConfiguration {

    private StepBuilderFactory stepBuilderFactory;
    private JobBuilderFactory jobBuilderFactory;
    private DataSource dataSource;

    public XmlJobConfiguration(
            StepBuilderFactory stepBuilderFactory,
            JobBuilderFactory jobBuilderFactory,
            DataSource dataSource) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(this.dataSource);
        reader.setFetchSize(10);
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
    public StaxEventItemWriter<Customer> staxEventItemWriter() throws Exception {

        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("customer", Customer.class);
        marshaller.setAliases(aliases);

        StaxEventItemWriter<Customer> writer = new StaxEventItemWriter<>();

        writer.setRootTagName("customers");
        writer.setMarshaller(marshaller);
        String customerOutputPath = File.createTempFile("customerOutput", ".xml").getAbsolutePath();
        System.out.println(">>: Output path - '{" + customerOutputPath + "}'");
        writer.setResource(new FileSystemResource(customerOutputPath));

        writer.afterPropertiesSet();

        return writer;
    }

    @Bean
    public Step xmlOutputStep() throws Exception {
        return stepBuilderFactory.get("xmlOuputStep")
                .<Customer, Customer>chunk(10)
                .reader(pagingItemReader())
                .writer(staxEventItemWriter())
                .build();
    }

    @Bean
    public Job jobOutputStep() throws Exception {
        return jobBuilderFactory.get("jobOutputStep")
                .start(xmlOutputStep())
                .build();
    }
}
