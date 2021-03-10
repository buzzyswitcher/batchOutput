package ru.volkov.batch.output.domain;

import org.springframework.jdbc.core.RowMapper;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class CustomerRowMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet resultSet, int i) throws SQLException {
        Customer customer = new Customer();
        customer.setId(resultSet.getInt("id"));
        customer.setName(resultSet.getString("name"));
        customer.setDate(toLocalDateTime(resultSet.getDate("date")));
        return customer;
    }

    LocalDateTime toLocalDateTime(Date dateToConvert) {
        return new java.sql.Timestamp(dateToConvert.getTime()).toLocalDateTime();
    }
}
