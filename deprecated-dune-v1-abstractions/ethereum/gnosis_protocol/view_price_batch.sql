BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_price_batch;
CREATE MATERIALIZED VIEW gnosis_protocol.view_price_batch AS
WITH token_priorities AS (
  SELECT * FROM (VALUES
        (100, 7), -- DAI
        (99, 2),  -- USDT
        (98, 4),  -- USDC
        (97, 3),  -- TUSD
        (96, 5),  -- PAX
        (95, 13), -- cDAI
        (94, 15), -- SNX
        (93, 17), -- PAXG
        (92, 9),  -- sUSD
        (91, 6),  -- GUSD
        (90, 16), -- CHAI
        (89, 1),  -- WETH
        (88, 8),  -- sETH
        (87, 11), -- sETH
        (86, 10), -- sBTC
        (85, 49), -- sEUR
        (84, 18)  -- GNO
    ) AS t (priority, token_id)
    -- ALL TOKENS: 	SELECT token_id, symbol FROM gnosis_protocol.view_tokens
),
solution AS (
  SELECT
  	batch_id,
  	evt_block_time,
  	evt_index,
  	token_id,
  	token_owl_price,
  	evt_block_number,
  	evt_tx_hash
  FROM (
  	SELECT
	  	FLOOR(EXTRACT(epoch FROM evt_block_time) / 300) - 1 AS batch_id,
	    -- The event time tells us the batch. Between minute 0-4 is resolved batch N-1
	    evt_block_time,
	    evt_index,
	    UNNEST(solution."tokenIdsForPrice") AS token_id,
	    UNNEST(solution.prices) AS token_owl_price,
	    evt_block_number,
	    evt_tx_hash,
	    RANK() OVER (
	      PARTITION BY FLOOR(EXTRACT(epoch FROM evt_block_time) / 300)
	      ORDER BY evt_block_time DESC, evt_index DESC
	    ) AS solution_rank
	  FROM gnosis_protocol. "BatchExchange_evt_SolutionSubmission" solution
  ) AS unique_solutions
  WHERE solution_rank = 1
),
solution_owl AS (
	SELECT DISTINCT
		batch_id,
		evt_block_time,
		evt_index,
		0::NUMERIC AS token_id,
		1000000000000000000::NUMERIC AS token_owl_price,
		evt_block_number,
		evt_tx_hash
	FROM solution
),
prices_in_owl AS (
  SELECT
    -- id
    solution.batch_id,
    solution.evt_index,
    tokens.token_id,
    -- tx/block
    solution.evt_block_number AS block_number,
    solution.evt_block_time AS block_time,
    solution.evt_tx_hash AS tx_hash,
    -- token
    tokens.token,
    tokens.symbol,
    tokens.decimals,
    -- price in OWL
    solution.token_owl_price / 10 ^(36 - COALESCE(tokens.decimals, 18)) AS token_owl_price
  FROM (
  	SELECT * FROM solution
  	UNION
  	SELECT * FROM solution_owl
  ) AS solution
  JOIN gnosis_protocol.view_tokens tokens ON solution.token_id = tokens.token_id
),
prices_in_usd AS (
  SELECT
    -- id
    batch_id,
    evt_index,
    token_id,
    -- prices
    token_owl_price,
    -- price of the token in USD
    usd_price.price AS token_usd_price,
    -- price of OWL in USD (by comparing the OWL price of the batch, with the actual price of this token)
    usd_price.price / token_owl_price AS owl_usd_price,
    -- tx/block
    block_number,
    block_time,
    tx_hash,
    -- token
    prices_in_owl.token,
    prices_in_owl.symbol,
    prices_in_owl.decimals
  FROM prices_in_owl
  LEFT OUTER JOIN prices.usd usd_price ON usd_price.contract_address = token
    AND usd_price.minute = DATE_TRUNC('minute', block_time) -- TODO: It can be slighly improved if we use the time of the "official" trade (end of the batch) instead of submission, but also more costly query
),
best_owl_price AS (
  SELECT
    batch_id,
    evt_index,
    owl_usd_price
  FROM (
      SELECT
        prices_in_usd.batch_id,
        prices_in_usd.evt_index,
        prices_in_usd.owl_usd_price,
        RANK() OVER (
          PARTITION BY batch_id,
          evt_index
          ORDER BY
            COALESCE(priority, 0) DESC
        ) AS price_rank
      FROM prices_in_usd
      LEFT OUTER JOIN token_priorities ON prices_in_usd.token_id = token_priorities.token_id
      WHERE token_usd_price IS NOT NULL
    ) AS ranked_owl_prices
  WHERE
    price_rank = 1
)
SELECT
  -- id
  prices_in_usd.batch_id,
  prices_in_usd.evt_index,
  prices_in_usd.token_id,
  -- price date
  TO_TIMESTAMP((prices_in_usd.batch_id + 1) * 300) AS price_date,
  -- Block / tx
  prices_in_usd.block_number AS block_number_solution,
  prices_in_usd.block_time AS block_time_solution,
  prices_in_usd.tx_hash AS tx_hash_solution,
  -- Token
  prices_in_usd.token,
  prices_in_usd.symbol,
  prices_in_usd.decimals,
  --  ---------------------------------
  --	Prices
  --- ---------------------------------
  --		"token_owl_price": TOKEN-OWL
  --				* Internal price of the token in OWL, ditacted by the solver
  --				* i.e Solver finds a solution saying WETH price is 210 OWL
  --		"owl_usd_price": OWL-USD
  --				* Best estimation price for OWL
  --				* Considers the external prices for all tokens to calculate the OWL-USD according to that token
  --				* For the tokens that the external price is known, selects the best price according to a defined token priority
  --				* As a fallback, if no token has an estimation, we assume the OWL cost a constant value --> 0.8$
  --				* i.e. OWL price according to DAI external source is 0.8$, according to WETH is 0.75$, then since DAI has more priority, we select 0.8$ as the price of OWL
  --		"token_usd_price_external":	TOKEN-USD
  --				* External price of the token in USD
  --				* It can be NULL, meaning, there's no price at that time (or price for the token at all)
  --				* i.e 250$ for WETH according to Binance
  --		"token_usd_price": OWL-USD
  --				* Is the best estimation we can give for the price in USD for the token
  --				* All the other prices are nice to know, but this one is the one we are really interested in. The other prices help us calculate this one
  --				* Calculates it using the price in OWL that reports the solver and our best estimate of OWL price in USD (owl_usd_price)
  --				* token_usd_price = token_owl_price * owl_usd_price
  --				* NOTE: Alternativelly, it could have been used the external token price if available, but it was prefered to use uniform OWL prices sol USD price are coherent with OWL prices (they keep the same proportion within the batch)
  prices_in_usd.token_owl_price,
  COALESCE(
    best_owl_price.owl_usd_price,
    0.95
  ) AS owl_usd_price,
  prices_in_usd.token_usd_price AS token_usd_price_external,
  prices_in_usd.token_owl_price * COALESCE(
    best_owl_price.owl_usd_price,
    0.95
  ) AS token_usd_price
FROM prices_in_usd
LEFT OUTER JOIN best_owl_price ON best_owl_price.batch_id = prices_in_usd.batch_id
  AND best_owl_price.evt_index = prices_in_usd.evt_index;


CREATE UNIQUE INDEX IF NOT EXISTS view_price_batch_id ON gnosis_protocol.view_price_batch (batch_id, token_id);
CREATE INDEX view_price_batch_idx_1 ON gnosis_protocol.view_price_batch (token_id);
CREATE INDEX view_price_batch_idx_2 ON gnosis_protocol.view_price_batch (symbol);
CREATE INDEX view_price_batch_idx_3 ON gnosis_protocol.view_price_batch (price_date);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_price_batch')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
