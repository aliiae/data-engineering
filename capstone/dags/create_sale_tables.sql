drop table if exists sale;
drop table if exists time;
drop table if exists property;
drop table if exists postcode;

create table if not exists "time"
(
    date    date not null primary key,
    day     int4,
    week    int4,
    month   int4,
    year    int4,
    weekday int4
);

create table if not exists property
(
    id       int4 identity (0, 1) not null primary key,
    type     varchar              not null,
    is_new   BOOLEAN,
    duration varchar
);

create table if not exists sale
(

    id          varchar(36) not null primary key,
    price       int4        not null,
    date        date        not null references time (date),
    property_id int4        not null references property (id),
    postcode    varchar(7)  not null references postcode (code),
    address     varchar,
    year        int4,
    month       int4
);
