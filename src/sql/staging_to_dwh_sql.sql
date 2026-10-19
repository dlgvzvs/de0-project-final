DROP SCHEMA IF EXISTS temp_name_DWH;

CREATE SCHEMA temp_name_DWH;

DROP TABLE IF EXISTS temp_name_DWH.global_metrics;

CREATE TABLE temp_name_DWH.global_metrics (
	date_update TIMESTAMP NOT NULL,
	currency_from SMALLINT NOT NULL, -- тут пока не опредеделился - либо temp_name_STAGING.currencies(currency_code), либо temp_name_STAGING.transactions(account_number_from)
	amount_total NUMERIC (18,2) NOT NULL CHECK (amount_total <> 0),
	cnt_transactions BIGINT NOT NULL CHECK (cnt_transactions >= 0),
	avg_transactions_per_account NUMERIC (10,2) CHECK (avg_transactions_per_account >= 0),
	cnt_accounts_make_transactions BIGINT NOT NULL CHECK (cnt_accounts_make_transactions >= 0),
	PRIMARY KEY (date_update, currency_from)
);

WITH aggregations_per_date_and_currency AS (
	SELECT
		DATE_TRUNC(transaction_dt, day) as date_update,
		currency_code AS currency_from,
		SUM(amount) AS amount_total,
		COUNT(operation_id) AS cnt_transactions,
		(COUNT(operation_id) / NULLIF(COUNT(DISTINCT account_number_from), 0))::NUMERIC (10,2) AS avg_transactions_per_account,
		COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
	FROM temp_name_STAGING.transactions
	WHERE account_number_from > 0 
	AND account_number_to > 0
	AND transaction_dt >= '{{ ds }}'::TIMESTAMP
	AND transaction_dt < ('{{ ds }}'::TIMESTAMP + INTERVAL '1 day')
	GROUP BY DATE_TRUNC(transaction_dt, day), currency_code
)

MERGE INTO temp_name_DWH.global_metrics AS target
USING (
    SELECT
        apdac.date_update,
        apdac.currency_from,
        apdac.amount_total * c.currency_with_div AS amount_total,
        apdac.cnt_transactions,
        apdac.avg_transactions_per_account,
        apdac.cnt_accounts_make_transactions
    FROM aggregations_per_date_and_currency apdac
    JOIN temp_name_STAGING.currencies c 
      ON apdac.currency_from = c.currency_code 
     AND apdac.date_update::DATE = c.date_update::DATE 
     AND c.currency_code_with = 410
) AS my_upsert_query
ON target.date_update = my_upsert_query.date_update 
AND target.currency_from = my_upsert_query.currency_from
WHEN MATCHED THEN
    UPDATE SET
        amount_total = my_upsert_query.amount_total,
        cnt_transactions = my_upsert_query.cnt_transactions,
        avg_transactions_per_account = my_upsert_query.avg_transactions_per_account,
        cnt_accounts_make_transactions = my_upsert_query.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
    INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    VALUES (my_upsert_query.date_update, my_upsert_query.currency_from, my_upsert_query.amount_total, my_upsert_query.cnt_transactions, my_upsert_query.avg_transactions_per_account, my_upsert_query.cnt_accounts_make_transactions);