CREATE TABLE IF NOT EXISTS synthetix.rates (
    currency_key	bytea		NOT NULL,
    currency_rate	numeric		NOT NULL,
    block_time	    timestamptz	NOT NULL,
    PRIMARY KEY(currency_key, block_time)
);

CREATE INDEX rates_block_time ON synthetix.rates (block_time);
CREATE INDEX rates_currency_key ON synthetix.rates (currency_key);
CREATE INDEX rates_currency_key_block_time ON synthetix.rates (currency_key, block_time DESC) INCLUDE (currency_rate);

INSERT INTO synthetix.rates VALUES (
    '\x7355534400000000000000000000000000000000000000000000000000000000'::bytea, 1000000000000000000::numeric,	'2019-03-11T22:17:52.000Z'::timestamptz
) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION synthetix.insert_rates(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO synthetix.rates
    SELECT currency_key, currency_rate, evt_block_time AS block_time
    FROM synthetix."ExchangeRates_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") AS u(currency_key, currency_rate)
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts

    UNION
    
    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN synthetix."ExchangeRates_evt_AggregatorAdded" agg ON agg.aggregator = cl.contract_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT currency_key, currency_rate, evt_block_time AS block_time
    FROM synthetix."ExchangeRates_v2_23_3_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") AS u(currency_key, currency_rate)
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN synthetix."ExchangeRates_v2_23_3_evt_AggregatorAdded" agg ON agg.aggregator = cl.contract_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN chainlink."view_aggregator_mappings" clam ON clam.aggregator_address = cl.contract_address
    INNER JOIN synthetix."ExchangeRates_v2_27_2_evt_AggregatorAdded" agg ON agg.aggregator = clam.proxy_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN chainlink."view_aggregator_mappings" clam ON clam.aggregator_address = cl.contract_address
    INNER JOIN synthetix."ExchangeRates_v2_28_4_evt_AggregatorAdded" agg ON agg.aggregator = clam.proxy_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN chainlink."view_aggregator_mappings" clam ON clam.aggregator_address = cl.contract_address
    INNER JOIN synthetix."ExchangeRates_v2_30_0_evt_AggregatorAdded" agg ON agg.aggregator = clam.proxy_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN chainlink."view_aggregator_mappings" clam ON clam.aggregator_address = cl.contract_address
    INNER JOIN synthetix."ExchangeRates_v2_31_1_evt_AggregatorAdded" agg ON agg.aggregator = clam.proxy_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts

    UNION

    SELECT
        agg."currencyKey" AS currency_key,
        cl.current * 1e10 AS currency_rate,
        cl.evt_block_time AS block_time
    FROM chainlink."Aggregator_evt_AnswerUpdated" cl
    INNER JOIN chainlink."view_aggregator_mappings" clam ON clam.aggregator_address = cl.contract_address
    INNER JOIN synthetix."ExchangeRates_v2_35_2_evt_AggregatorAdded" agg ON agg.aggregator = clam.proxy_address
    WHERE cl.evt_block_time >= start_ts
    AND cl.evt_block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


SELECT synthetix.insert_rates('2019-01-01', (SELECT max(time) FROM ethereum.blocks));

INSERT INTO cron.job (schedule, command)
VALUES ('*/5 * * * *', 'SELECT synthetix.insert_rates((SELECT max(block_time) FROM synthetix.rates));')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
