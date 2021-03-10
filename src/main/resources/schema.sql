drop table if exists output.customers;

create schema output;

create table output.customers(
    id serial not null,
    name varchar,
    date timestamp,
    card_bsk_num integer,
    constraint customers_pk primary key (id)
);