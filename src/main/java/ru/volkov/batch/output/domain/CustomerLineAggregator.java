package ru.volkov.batch.output.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.transform.LineAggregator;

public class CustomerLineAggregator implements LineAggregator<Customer> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String aggregate(Customer customer) {
        try {
            return mapper.writeValueAsString(customer);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Uable to serialize object: " + e.getMessage());
        }
    }
}
