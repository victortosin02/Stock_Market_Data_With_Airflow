CREATE SCHEMA IF NOT EXIST stocks;

DROP TABLE IF EXISTS stocks.historical_stock_data;

CREATE TABLE stocks.historical_stock_data(
    id BIGSERIAL PRIMARY KEY,
    stock_date varchar(56) NULL,
    open_value VARCHAR(56) NULL,
    high_value VARCHAR(56) NULL,
    low_value VARCHAR(56) NULL,
    close_value VARCHAR(56) NULL,
    volume_traded VARCHAR(56) NULL,
    daily_percent_change VARCHAR(56) NULL,
    value_change VARCHAR(56) NULL,
    company_name VARCHAR(56) NULL
);

SELECT aws_s3.table_import_from_s3(
    'stocks.historical_stock_data',
    'stock_date, open_value, high_value, low_value, close_value, volume_traded, daily_percent_change, value_change, company_name',
    '(format csv, DELIMITER '','', HEADER false)',
    'de-mbd-predict-victor-oladejo-s3-source',
    'Output/historical_stock_data.csv',
    'eu-west-1'
);