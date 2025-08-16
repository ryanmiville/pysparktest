CREATE TABLE IF NOT EXISTS my_db.my_table (
    id INT,
    name STRING,
    date STRING
) USING iceberg
PARTITIONED BY (date)