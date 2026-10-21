DROP SCHEMA IF EXISTS VT260701BC766E_STAGING; -- в качестве временной меры указал БД из проекта спринта "Аналитические базы данных"

CREATE SCHEMA VT260701BC766E_STAGING;

DROP TABLE IF EXISTS VT260701BC766E_STAGING.currencies;

CREATE TABLE VT260701BC766E_STAGING.currencies(
	currency_code SMALLINT,
	currency_code_with SMALLINT,
	date_update DATE,
	currency_with_div NUMERIC(3,2)
);

DROP PROJECTION IF EXISTS VT260701BC766E_STAGING.currencies;

CREATE PROJECTION VT260701BC766E_STAGING.currencies 
(currency_code,
 currency_code_with,
 date_update,
 currency_with_div) AS
SELECT currencies.currency_code,
	   currencies.currency_code_with,
	   currencies.date_update,
	   currencies.currency_with_div
FROM VT260701BC766E_STAGING.currencies
PARTITION BY currencies.date_update
ORDER BY currencies.date_update ASC
SEGMENTED BY HASH (currencies.date_update) ALL NODES KSAFE 1;


DROP TABLE IF EXISTS VT260701BC766E_STAGING.transactions;

CREATE TABLE VT260701BC766E_STAGING.transactions(
	operation_id UUID NOT NULL PRIMARY KEY,
	account_number_from BIGINT NOT NULL,
	account_number_to BIGINT NOT NULL,
	currency_code SMALLINT,
	country VARCHAR(20),
	status VARCHAR(25),
	transaction_type VARCHAR(30),
	amount NUMERIC (15,2) NOT NULL CHECK (amount <> 0),
	transaction_dt TIMESTAMP NOT NULL
);

DROP PROJECTION IF EXISTS VT260701BC766E_STAGING.transactions;

CREATE PROJECTION VT260701BC766E_STAGING.transactions
(operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt) AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
FROM VT260701BC766E_STAGING.transactions
PARTITION BY transactions.transaction_dt
ORDER BY transactions.transaction_dt ASC
SEGMENTED BY HASH(transactions.operation_id) ALL NODES KSAFE 1;

-- Загрузка данных будет в даге