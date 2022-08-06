DROP TABLE IF EXISTS hive_table;

CREATE TABLE hive_table
    (CustomerId          BIGINT,
    Gender               STRING,
    State                STRING,
    AccountNumber        STRING,
    TypeOfAccount        STRING,
    CreatedAt            STRING,
    Name                 STRING,
    CreatedAtYear        BIGINT,
    DeletedAt            STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA
INPATH '/processed/*.csv'
OVERWRITE
INTO TABLE hive_table;

CREATE EXTERNAL TABLE if not exists CustomerAccountDDB
    (CustomerId             BIGINT,
    Gender                  STRING ,
    State                   STRING ,
    AccountNumber           STRING,
    TypeOfAccount           STRING,
    CreatedAt               STRING,
    Name                    STRING,
    CreatedAtYear           BIGINT,
    DeletedAt                 STRING)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "CustomerAccount",
    "dynamodb.column.mapping"="CustomerId:CustomerId,Gender:Gender,State:State,AccountNumber:AccountNumber,TypeOfAccount:TypeOfAccount,CreatedAt:CreatedAt,Name:Name,CreatedAtYear:CreatedAtYear,DeletedAt:DeletedAt");

insert into table CustomerAccountDDB
SELECT c.customerid, c.gender, c.state, c.accountnumber, c.typeofaccount, c.createdat, c.name, c.createdatyear, current_date
FROM CustomerAccountDDB c LEFT JOIN hive_table h on h.AccountNumber = c.AccountNumber
where h.AccountNumber is null and c.DeletedAt is null;

INSERT INTO TABLE CustomerAccountDDB
SELECT * FROM hive_table;

quit;