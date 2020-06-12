create table if not exists property
(
    id       INTEGER identity (0, 1) not null primary key,
    type     VARCHAR                 not null,
    is_new   BOOLEAN,
    duration VARCHAR
);

copy property (id, type, is_new, duration) from 's3://capstone-price-paid/property.parquet' iam_role 'arn:aws:iam::945096716378:role/myRedshiftRole' format as parquet;
