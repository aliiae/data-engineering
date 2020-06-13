create table if not exists postcode
(
    code      varchar(7) not null primary key,
    district  varchar    not null,
    country   varchar    not null,
    latitude  FLOAT      not null,
    longitude FLOAT      not null
);
