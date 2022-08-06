select * from customers;
select * from accounts;

--null value
select * from customers 
where first_name is null
or last_name is null
or gender is null
or state is null;

select * from accounts 
where account_number is null
or type_of_account is null;

select concat(round((count(*)-count(first_name))/count(*)::decimal*100,2), '%') as "% first_name is null",
concat(round((count(*)-count(last_name))/count(*)::decimal*100,2), '%') as "% last_name is null",
concat(round((count(*)-count(gender))/count(*)::decimal*100,2), '%') as "% gender is null",
concat(round((count(*)-count(state))/count(*)::decimal*100,2), '%') as "% state is null" from customers;

select concat(round((count(*)-count(account_number))/count(*)::decimal*100, 2),'%') as "% account_number is null",
concat(round((count(*)-count(type_of_account))/count(*)::decimal*100, 2), '%') as "% type_of_account is null" from accounts;

--duplicates
select count(*) as number_of_duplicates from customers c where 
exists(
	select count(*) from customers where c.customer_id = customer_id
	group by customer_id 
	having count(*) > 1
);

select count(*) as number_of_duplicates from accounts a where 
exists(
	select count(*) from accounts where a.account_number = account_number
	group by account_number
	having count(*) > 1
);

--old account type
select round(count(*)/ (select count(*) from customers)::decimal,2)*100 as "% legacy accounts" from accounts where type_of_account not in ('checking', 'forex', 'retirement', 'saving', 'trading');

--spelling errors
select round(count(*)/(select count(*) from customers)::decimal,2)*100 as "% misspelled state names" from customers where state not in ('Alabama', 'Alaska', 'Arizona', 'Arkansas',	'California', 'Colorado', 
												'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 
												'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 
												'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 
												'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 
												'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 
												'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 
												'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 
												'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 
												'West Virginia', 'Wisconsin', 'Wyoming'
);

--volumn
select pg_size_pretty(pg_database_size('aaco')) as "size of database", 
pg_size_pretty(pg_table_size('customers')) as "size of customers table",
pg_size_pretty(pg_table_size('accounts')) as "size of customers table";

--how many rows
select count(*) from customers;

--how many columns
select count(*) from information_schema.columns where table_name='customers';

--how many customers
select count(customer_id) as "total rows", count(distinct customer_id) as "num of customers" from customers;

select count(customer_id) as "total rows", count(distinct customer_id) as "num of customers" from accounts;

--customers by geo
select count(distinct state) from customers;
select state, count(distinct customer_id) as "customers' geo seg" from customers where state is not null group by state;

--customers by gender
select gender, count(distinct customer_id) from customers where gender is not null group by gender;

--customer by type of accounts
select type_of_account, count(distinct customer_id) from accounts group by type_of_account;