{{ config(
    materialized = 'view',
    alias = 'trades'
) }}

WITH dexs AS
(
    -- Curvefi TokenExchange
    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN bought_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN sold_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_swap_evt_TokenExchange') }}

    UNION

    -- Curvefi TokenExchangeUnderlying
    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN bought_id = 3 THEN '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN sold_id = 3 THEN '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xF61718057901F84C4eEC4339EF8f0D86D2B45600'::bytea
            WHEN bought_id = 1 THEN '\xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xF61718057901F84C4eEC4339EF8f0D86D2B45600'::bytea
            WHEN sold_id = 1 THEN '\xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xF61718057901F84C4eEC4339EF8f0D86D2B45600'::bytea
            WHEN bought_id = 1 THEN '\xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xF61718057901F84C4eEC4339EF8f0D86D2B45600'::bytea
            WHEN sold_id = 1 THEN '\xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN bought_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN sold_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN bought_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN sold_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_v2_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_v2_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN bought_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN sold_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN bought_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'::bytea
            WHEN sold_id = 1 THEN '\x39AA39c021dfbaE8faC545936693aC917d5E7563'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'usdt_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'usdt_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01'::bytea
            WHEN bought_id = 1 THEN '\xd6aD7a6750A7593E092a9B218d66C0A814a3436e'::bytea
            WHEN bought_id = 2 THEN '\x83f798e925BcD4017Eb265844FDDAbb448f1707D'::bytea
            WHEN bought_id = 3 THEN '\x73a052500105205d34Daf004eAb301916DA8190f'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01'::bytea
            WHEN sold_id = 1 THEN '\xd6aD7a6750A7593E092a9B218d66C0A814a3436e'::bytea
            WHEN sold_id = 2 THEN '\x83f798e925BcD4017Eb265844FDDAbb448f1707D'::bytea
            WHEN sold_id = 3 THEN '\x73a052500105205d34Daf004eAb301916DA8190f'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'y_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN bought_id = 3 THEN '\x0000000000085d4780B73119b644AE5ecd22b376'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN sold_id = 3 THEN '\x0000000000085d4780B73119b644AE5ecd22b376'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'y_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xC2cB1040220768554cf699b0d863A3cd4324ce32'::bytea
            WHEN bought_id = 1 THEN '\x26EA744E5B887E5205727f55dFBE8685e3b21951'::bytea
            WHEN bought_id = 2 THEN '\xE6354ed5bC4b393a5Aad09f21c46E101e692d447'::bytea
            WHEN bought_id = 3 THEN '\x04bC0Ab673d88aE9dbC9DA2380cB6B79C4BCa9aE'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xC2cB1040220768554cf699b0d863A3cd4324ce32'::bytea
            WHEN sold_id = 1 THEN '\x26EA744E5B887E5205727f55dFBE8685e3b21951'::bytea
            WHEN sold_id = 2 THEN '\xE6354ed5bC4b393a5Aad09f21c46E101e692d447'::bytea
            WHEN sold_id = 3 THEN '\x04bC0Ab673d88aE9dbC9DA2380cB6B79C4BCa9aE'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'busd_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN bought_id = 3 THEN '\x4Fabb145d64652a948d72533023f6E7A623C7C53'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN sold_id = 3 THEN '\x4Fabb145d64652a948d72533023f6E7A623C7C53'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'busd_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x99d1Fa417f94dcD62BfE781a1213c092a47041Bc'::bytea
            WHEN bought_id = 1 THEN '\x9777d7E2b60bB01759D0E2f8be2095df444cb07E'::bytea
            WHEN bought_id = 2 THEN '\x1bE5d71F2dA660BFdee8012dDc58D024448A0A59'::bytea
            WHEN bought_id = 3 THEN '\x8E870D67F660D95d5be530380D0eC0bd388289E1'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x99d1Fa417f94dcD62BfE781a1213c092a47041Bc'::bytea
            WHEN sold_id = 1 THEN '\x9777d7E2b60bB01759D0E2f8be2095df444cb07E'::bytea
            WHEN sold_id = 2 THEN '\x1bE5d71F2dA660BFdee8012dDc58D024448A0A59'::bytea
            WHEN sold_id = 3 THEN '\x8E870D67F660D95d5be530380D0eC0bd388289E1'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'pax_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN bought_id = 3 THEN '\x8E870D67F660D95d5be530380D0eC0bd388289E1'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
            WHEN sold_id = 3 THEN '\x8E870D67F660D95d5be530380D0eC0bd388289E1'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'pax_swap_evt_TokenExchangeUnderlying') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea
            WHEN bought_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea
            WHEN sold_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'ren_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea
            WHEN bought_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
            WHEN bought_id = 2 THEN '\xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea
            WHEN sold_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
            WHEN sold_id = 2 THEN '\xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'sbtc_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            --change address back to renBTC's, right now Dune only tracks WBTC price
            WHEN bought_id = 0 THEN '\xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'::bytea
            WHEN bought_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
        END as token_a_address,
        CASE
            --change address back to renBTC's, right now Dune only tracks WBTC price
            WHEN sold_id = 0 THEN '\xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'::bytea
            WHEN sold_id = 1 THEN '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'hbtc_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN bought_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN bought_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea
            WHEN sold_id = 1 THEN '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea
            WHEN sold_id = 2 THEN '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'threepool_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        NULL::text AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            WHEN bought_id = 1 THEN '\xae7ab96520de3a18e5e111b5eaab095312d7fe84'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            WHEN sold_id = 1 THEN '\xae7ab96520de3a18e5e111b5eaab095312d7fe84'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'steth_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        '2' AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN bought_id = 1 THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
            WHEN bought_id = 2 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN sold_id = 1 THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
            WHEN sold_id = 2 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'tricrypto_swap_evt_TokenExchange') }}

    UNION

    SELECT
        evt_block_time AS block_time,
        'Curve' AS project,
        '2' AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought AS token_a_amount_raw,
        tokens_sold AS token_b_amount_raw,
        CASE
            WHEN bought_id = 0 THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN bought_id = 1 THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
            WHEN bought_id = 2 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        END as token_a_address,
        CASE
            WHEN sold_id = 0 THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN sold_id = 1 THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
            WHEN sold_id = 2 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        END as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'tricrypto2_swap_evt_TokenExchange') }}   
)

SELECT
    'ethereum' AS blockchain
    ,'Curve' AS project
    ,'1' AS version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw
    ,dexs.token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
    ,'curve' ||'-'|| '1' ||'-'|| dexs.tx_hash ||'-'|| IFNULL(dexs.evt_index, '') ||'-'|| IFNULL(dexs.trace_address, '') AS unique_trade_id
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND tx.block_time >= "2018-11-01 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time = date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_bought.minute >= "2018-11-01 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_sold.minute >= "2018-11-01 00:00:00"
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
