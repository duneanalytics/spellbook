CREATE TABLE dex.trades (
    block_time timestamptz NOT NULL,
    token_a_symbol text,
    token_b_symbol text,
    token_a_amount numeric,
    token_b_amount numeric,
    project text NOT NULL,
    version text,
    trader_a bytea,
    trader_b bytea,
    token_a_amount_raw numeric,
    token_b_amount_raw numeric,
    usd_amount numeric,
    token_a_address bytea,
    token_b_address bytea,
    exchange_contract_address bytea NOT NULL,
    tx_hash bytea NOT NULL,
    tx_from bytea NOT NULL,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE OR REPLACE FUNCTION dex.insert_trades(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        trace_address,
        evt_index,
        trade_id
    )
    SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        project,
        version,
        coalesce(trader_a, tx."from") as trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id
    FROM (
        -- Uniswap v1 TokenPurchase
        SELECT
            t.evt_block_time AS block_time,
            'Uniswap' AS project,
            '1' AS version,
            buyer AS trader_a,
            NULL::bytea AS trader_b,
            tokens_bought token_a_amount_raw,
            eth_sold token_b_amount_raw,
            NULL::numeric AS usd_amount,
            f.token AS token_a_address,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token_b_address, --Using WETH for easier joining with USD price table
            t.contract_address exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            uniswap. "Exchange_evt_TokenPurchase" t
        INNER JOIN uniswap. "Factory_evt_NewExchange" f ON f.exchange = t.contract_address

        UNION

        -- Uniswap v1 EthPurchase
        SELECT
            t.evt_block_time AS block_time,
            'Uniswap' AS project,
            '1' AS version,
            buyer AS trader_a,
            NULL::bytea AS trader_b,
            eth_bought token_a_amount_raw,
            tokens_sold token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea token_a_address, --Using WETH for easier joining with USD price table
            f.token AS token_b_address,
            t.contract_address exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            uniswap. "Exchange_evt_EthPurchase" t
        INNER JOIN uniswap. "Factory_evt_NewExchange" f ON f.exchange = t.contract_address

        UNION

        -- Uniswap v2
        SELECT
            t.evt_block_time AS block_time,
            'Uniswap' AS project,
            '2' AS version,
            t."to" AS trader_a,
            NULL::bytea AS trader_b,
            CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
            CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
            CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
            t.contract_address exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            uniswap_v2."Pair_evt_Swap" t
        INNER JOIN uniswap_v2."Factory_evt_PairCreated" f ON f.pair = t.contract_address
        WHERE t.contract_address != '\xed9c854cb02de75ce4c9bba992828d6cb7fd5c71' --Remove WETH-UBOMB wash trading pair

        UNION

        -- Kyber: trade from Token - ETH
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '1' AS version,
            trader AS trader_a,
            NULL::bytea AS trader_b,
            CASE
                WHEN src IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
                ELSE "srcAmount"
            END AS token_a_amount_raw,
            "ethWeiValue" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            src AS token_a_address,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM
            kyber."Network_evt_KyberTrade"
        WHERE src NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

        UNION

        -- Kyber: trade from ETH - Token
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '1' AS version,
            trader AS trader_a,
            NULL::bytea AS trader_b,
            "ethWeiValue" AS token_a_amount_raw,
            CASE
                WHEN dest IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
                ELSE "dstAmount"
            END AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address,
            dest AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM
            kyber."Network_evt_KyberTrade"
        WHERE dest NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

        UNION

        --- Kyber_V2
        -- trade from token -eth
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '2' AS version,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            (SELECT SUM(s) FROM UNNEST(ARRAY((
                -- formula: https://github.com/KyberNetwork/smart-contracts/blob/Katalyst/contracts/sol6/utils/Utils5.sol#L88
                SELECT
                    CASE WHEN 18 >= src_token.decimals -- eth decimal
                        THEN a*b*power(10, (18-src_token.decimals))/1e18
                        ELSE (a*b) / (1e18 * power(10, (src_token.decimals - 18)))
                    END
                FROM unnest("t2eSrcAmounts", "t2eRates") AS t(a,b)
                ))) s
            ) AS token_a_amount_raw,
            (SELECT SUM(a) FROM UNNEST("t2eSrcAmounts") AS a) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address, -- trade from token - eth, dest should be weth
            src AS token_b_address,
            trade.contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM kyber_v2."Network_evt_KyberTrade" trade
        INNER JOIN erc20."tokens" src_token ON trade.src = src_token.contract_address
        AND src_token.contract_address != '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'

        UNION

        -- trade from eth - token
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '2' AS version,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            (SELECT SUM(s) FROM UNNEST(ARRAY((
                -- formula: https://github.com/KyberNetwork/smart-contracts/blob/Katalyst/contracts/sol6/utils/Utils5.sol#L88
                SELECT
                    CASE WHEN dst_token.decimals >= 18 -- eth decimal
                        THEN a*b*power(10, (dst_token.decimals-18))/1e18
                        ELSE (a*b) / (1e18 * power(10, (18- dst_token.decimals)))
                    END
                FROM unnest("e2tSrcAmounts", "e2tRates") AS t(a,b)
                ))) s
            ) AS token_a_amount_raw,
            (SELECT SUM(a) FROM UNNEST("e2tSrcAmounts") AS a) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            dest AS token_a_address,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address, -- trade from eth - token, src should be weth
            trade.contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM kyber_v2."Network_evt_KyberTrade" trade
        INNER JOIN erc20."tokens" dst_token ON trade.dest = dst_token.contract_address
        AND dst_token.contract_address != '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        
        UNION

        -- Old Oasis (eth2dai) contract
        SELECT
            t.evt_block_time AS block_time,
            'Oasis' AS project,
            '1' AS version,
            take.taker AS trader_a,
            take.maker AS trader_b,
            t.buy_amt AS token_a_amount_raw,
            t.pay_amt AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t.buy_gem AS token_a_address,
            t.pay_gem AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM oasisdex."eth2dai_evt_LogTrade" t
        LEFT JOIN LATERAL (
            SELECT taker, maker
            FROM oasisdex."eth2dai_evt_LogTake" take
            WHERE t.evt_tx_hash = take.evt_tx_hash
            AND take.evt_index > t.evt_index
            ORDER BY take.evt_index ASC
            LIMIT 1
        ) take
        ON TRUE

        UNION

        -- Oasis contract
        SELECT
            t.evt_block_time AS block_time,
            'Oasis' AS project,
            '2' AS version,
            take.taker AS trader_a,
            take.maker AS trader_b,
            t.buy_amt AS token_a_amount_raw,
            t.pay_amt AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t.buy_gem AS token_a_address,
            t.pay_gem AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM oasisdex."MatchingMarket_evt_LogTrade" t
        LEFT JOIN LATERAL (
            SELECT taker, maker
            FROM oasisdex."MatchingMarket_evt_LogTake" take
            WHERE t.evt_tx_hash = take.evt_tx_hash
            AND take.evt_index > t.evt_index
            ORDER BY take.evt_index ASC
            LIMIT 1
        ) take
        ON TRUE

        UNION

        -- 0x v2.1
        SELECT
            evt_block_time AS block_time,
            '0x' AS project,
            '2.1' AS version,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_raw,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex_v2."Exchange2.1_evt_Fill"

        UNION

        -- 0x v3
        SELECT
            evt_block_time AS block_time,
            '0x' AS project,
            '3' AS version,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_rawr,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex_v3."Exchange_evt_Fill"

        UNION

        -- dYdX Solo Margin v2
        SELECT
            evt_block_time AS block_time,
            'dYdX' AS project,
            'Solo Margin v2' AS version,
            "takerAccountOwner" AS trader_a,
            "makerAccountOwner" AS trader_b,
            abs(("takerOutputUpdate"->'deltaWei'->'value')::numeric)/2 AS token_a_amount_raw, --"takerOutputNumber"
            abs(("takerInputUpdate"->'deltaWei'->'value')::numeric)/2 AS token_b_amount_raw, --"takerInputNumber"
            NULL::numeric AS usd_amount,
            CASE
                WHEN "outputMarket" = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
                WHEN "outputMarket" = 1 THEN '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'::bytea
                WHEN "outputMarket" = 2 THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
                WHEN "outputMarket" = 3 THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
            END AS token_a_address,
            CASE
                WHEN "inputMarket" = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
                WHEN "inputMarket" = 1 THEN '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'::bytea
                WHEN "inputMarket" = 2 THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
                WHEN "inputMarket" = 3 THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
            END AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM dydx."SoloMargin_evt_LogTrade"

        UNION

        -- dYdX PBTC-USDC Perpetual
        SELECT
            evt_block_time AS block_time,
            'dYdX' AS project,
            'PBTC-USDC Perpetual' AS version,
            maker AS trader_a,
            taker AS trader_b,
            "positionAmount" AS token_a_amount_raw,
            "marginAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' AS token_a_address,
            '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dydx_perpetual."PerpetualV1_evt_LogTrade"
        WHERE "isBuy" = 'True'
        AND contract_address = '\x07aBe965500A49370D331eCD613c7AC47dD6e547'
                                       
        UNION
                                       
        -- dYdX WETH-PUSD Perpetual
        SELECT
            evt_block_time AS block_time,
            'dYdX' AS project,
            'WETH-PUSD Perpetual' AS version,
            maker AS trader_a,
            taker AS trader_b,
            "marginAmount" AS token_a_amount_raw,
            NULL::numeric AS token_b_amount_raw,
            "positionAmount"/1e6 AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token_a_address,
            NULL::bytea AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dydx_perpetual."PerpetualV1_evt_LogTrade"
        WHERE "isBuy" = 'True'
        AND contract_address = '\x09403FD14510F8196F7879eF514827CD76960B5d'

        UNION

        -- Loopring v3.1
        (
            WITH trades AS (
                SELECT loopring.fn_process_trade_block(CAST(b."blockSize" AS INT), b._3, b.call_block_time) AS trade,
                    b."contract_address" AS exchange_contract_address,
                    b.call_tx_hash AS tx_hash,
                    b.call_trace_address AS trace_address,
                    NULL::bigint AS evt_index
                FROM loopring."DEXBetaV1_call_commitBlock" b
                WHERE b."blockType" = '0'
            ), token_table AS (
                SELECT 0 AS "token_id", '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token
                UNION
                SELECT "tokenId" AS "token_id", "token"
                FROM loopring."DEXBetaV1_evt_TokenRegistered" e
                WHERE token != '\x0000000000000000000000000000000000000000'
            )
            SELECT (t.trade).block_timestamp AS block_time,
                'Loopring' AS project,
                '3.1' AS version,
                (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountA) AS trader_a,
                (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountB) AS trader_B,
                (t.trade).fillA::numeric AS token_a_amount_raw,
                (t.trade).fillB::numeric AS token_b_amount_raw,
                NULL::numeric AS usd_amount,
                (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenA) AS token_a_address,
                (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenB) AS token_b_address,
                exchange_contract_address,
                tx_hash,
                trace_address,
                evt_index
            FROM trades t
        )

        UNION

        -- 1inch
        SELECT
            oi.block_time,
            '1inch' AS project,
            '1' AS version,
            tx_from AS trader_a,
            NULL::bytea AS trader_b,
            from_amount AS token_a_amount_raw,
            to_amount AS token_b_amount_raw,
            GREATEST(from_usd, to_usd) AS usd_amount,
            from_token AS token_a_address,
            to_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM (
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, trace_address, NULL::integer AS evt_index FROM oneinch.view_swaps
            UNION ALL
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, trace_address, NULL::integer AS evt_index FROM onesplit.view_swaps
            WHERE tx_hash NOT IN (SELECT tx_hash FROM oneinch.view_swaps)
            UNION ALL
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, NULL::integer[] AS trace_address, evt_index FROM oneproto.view_swaps
            WHERE tx_hash NOT IN (SELECT tx_hash FROM oneinch.view_swaps)
        ) oi

        UNION

        -- IDEX v1
        SELECT
            call_block_time AS block_time,
            'IDEX' AS project,
            '1' AS version,
            "tradeAddresses"[3] AS trader_a,
            "tradeAddresses"[4] AS trader_b,
            "tradeValues"[1] AS token_a_amount_raw,
            "tradeValues"[2] AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE WHEN "tradeAddresses"[1] = '\x0000000000000000000000000000000000000000' THEN
                '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            ELSE "tradeAddresses"[1]
            END AS token_a_address,
            CASE WHEN "tradeAddresses"[2] = '\x0000000000000000000000000000000000000000' THEN
                '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            ELSE "tradeAddresses"[2]
            END AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash AS tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM idex."IDEX1_call_trade"
        WHERE call_success

        UNION

        -- Mooniswap
        SELECT
            evt_block_time,
            'Mooniswap' AS project,
            '1' AS version,
            account AS trader_a,
            NULL::bytea AS trader_b,
            amount AS token_a_amount_raw,
            result AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            src AS token_a_address,
            dst AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM mooniswap."MooniSwap_evt_Swapped"

        UNION

        --Curve
        SELECT
            block_time,
            project,
            version,
            trader_a,
            trader_b,
            token_a_amount_raw,
            token_b_amount_raw,
            NULL::numeric AS usd_amount,
            token_a_address,
            token_b_address,
            exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM curvefi.view_trades

        UNION

        -- Balancer
        SELECT
            t.evt_block_time AS block_time,
            'Balancer' AS project,
            '1' AS version,
            t.caller AS trader_a,
            NULL::bytea AS trader_b,
            t."tokenAmountIn" AS token_a_amount_raw,
            t."tokenAmountOut" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."tokenIn" token_a_address,
            t."tokenOut" token_b_address,
            t.contract_address exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            balancer."BPool_evt_LOG_SWAP" t
        INNER JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = t.contract_address

        UNION

        --DDEX
        SELECT
            evt_block_time AS block_time,
            'DDEX' AS project,
            NULL AS version,
            buyer AS trader_a,
            CASE
                WHEN buyer = maker
                THEN taker ELSE maker
            END AS trader_b,
            "quoteAssetFilledAmount" AS token_a_amount_raw,
            "baseAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE
                WHEN "addressSet"->>'quoteAsset' = '0x000000000000000000000000000000000000000e' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE decode(substring(("addressSet"->'quoteAsset')::TEXT, 4,40), 'hex')
            END AS token_a_address,
            CASE
                WHEN "addressSet"->>'baseAsset' = '0x000000000000000000000000000000000000000e' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE decode(substring(("addressSet"->'baseAsset')::TEXT, 4,40), 'hex')
            END AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM hydroprotocol."Margin_evt_Match"

        UNION

        -- Gnosis Protocol
        SELECT
            block_time,
            'Gnosis Protocol' AS project,
            '1' AS version,
            trader_hex AS trader_a,
            NULL::bytea AS trader_b,
            sell_amount_atoms / 2 AS token_a_amount_raw,
            buy_amount_atoms / 2 AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            sell_token AS token_a_address,
            buy_token AS token_b_address,
            '\x6F400810b62df8E13fded51bE75fF5393eaa841F' AS exchange_contract_address,
            tx_hash,
            NULL::integer[] AS trace_address,
            evt_index_trades
        FROM gnosis_protocol.view_trades

        UNION

        -- Bancor Network
        SELECT
            block_time,
            'Bancor Network' AS project,
            NULL AS version,
            trader AS trader_a,
            NULL::bytea AS trader_b,
            target_token_amount_raw AS token_a_amount_raw,
            source_token_amount_raw AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE WHEN target_token_address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN
                '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            ELSE target_token_address
            END AS token_a_address,
            CASE WHEN source_token_address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN
                '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            ELSE source_token_address
            END AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM bancornetwork.view_convert
                                            
        UNION

        -- Sushiswap
        SELECT
            t.evt_block_time AS block_time,
            'Sushiswap' AS project,
            '1' AS version,
            t."to" AS trader_a,
            NULL::bytea AS trader_b,
            CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
            CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
            CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
            t.contract_address exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            sushi."Pair_evt_Swap" t
        INNER JOIN sushi."Factory_evt_PairCreated" f ON f.pair = t.contract_address
                        
    ) dexs
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    INNER JOIN ethereum.transactions tx 
        ON dexs.tx_hash = tx.hash 
        AND tx.block_time >= start_ts 
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts

    UNION

    -- synthetix has their own usd-prices
    SELECT
        tr.block_time,
        a.symbol AS token_a_symbol,
        b.symbol AS token_b_symbol,
        token_a_amount,
        token_b_amount,
        'Synthetix' AS project,
        NULL AS version,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        token_a_amount_usd AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        NULL AS trace_address,
        evt_index,
        trade_id
    FROM synthetix.trades tr
    LEFT JOIN synthetix.symbols a ON tr.token_a_address = a.address
    LEFT JOIN synthetix.symbols b ON tr.token_b_address = b.address
    INNER JOIN ethereum.transactions tx
        ON tr.tx_hash = tx.hash
        AND tx.block_time >= start_ts 
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    WHERE tr.block_time >= start_ts
    AND tr.block_time < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS dex_trades_tr_addr_uniq_idx ON dex.trades (tx_hash, trace_address, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS dex_trades_evt_index_uniq_idx ON dex.trades (tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS dex_trades_tx_from_idx ON dex.trades (tx_from);
CREATE INDEX IF NOT EXISTS dex_trades_project_idx ON dex.trades (project);
CREATE INDEX IF NOT EXISTS dex_trades_block_time_idx ON dex.trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS dex_trades_token_a_idx ON dex.trades (token_a_address);
CREATE INDEX IF NOT EXISTS dex_trades_token_b_idx ON dex.trades (token_b_address);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$SELECT dex.insert_trades((SELECT max(block_time) - interval '1 days' FROM dex.trades), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
