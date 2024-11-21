{{ config(
        
        schema = 'aave_v3'
        , alias = 'tokens'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'atoken_address']
        , post_hook='{{ expose_spells(\'["optimism","polygon","arbitrum","avalanche_c"]\',
                                  "project",
                                  "aave_v3",
                                  \'["msilb7"]\') }}'
  )
}}

-- chains where aave v3 contracts are decoded.
{% set aave_v3_decoded_chains = [
    'optimism',
    'polygon',
    'arbitrum',
    'avalanche_c'
] %}

SELECT distinct a.blockchain
              , a.atoken_address
              , a.underlying_address
              , a.atoken_decimals
              , a.side
              , a.arate_type
              , a.atoken_symbol
              , a.atoken_name

FROM (
    {% for aave_v3_chain in aave_v3_decoded_chains %}
        SELECT
            '{{aave_v3_chain}}' AS blockchain
            , contract_address AS atoken_address
            , underlyingAsset AS underlying_address
            , aTokenDecimals AS atoken_decimals
            , 'Supply' AS side
            , 'Variable' AS arate_type
            , aTokenSymbol AS atoken_symbol
            , aTokenName AS atoken_name
        FROM {{source( 'aave_v3_' + aave_v3_chain, 'AToken_evt_Initialized' ) }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        SELECT
            '{{aave_v3_chain}}' AS blockchain
            , contract_address AS atoken_address
            , underlyingAsset AS underlying_address
            , debtTokenDecimals AS atoken_decimals
            , 'Borrow' AS side
            , 'Stable' AS arate_type
            , debtTokenSymbol AS atoken_symbol
            , debtTokenName AS atoken_name
        FROM {{source( 'aave_v3_' + aave_v3_chain, 'StableDebtToken_evt_Initialized' ) }}
        WHERE debtTokenName LIKE '%Stable%'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        SELECT
            '{{aave_v3_chain}}' AS blockchain
            , contract_address AS atoken_address
            , underlyingAsset AS underlying_address
            , debtTokenDecimals AS atoken_decimals
            , 'Borrow' AS side
            , 'Variable' AS arate_type
            , debtTokenSymbol AS atoken_symbol
            , debtTokenName AS atoken_name
        FROM {{source( 'aave_v3_' + aave_v3_chain, 'VariableDebtToken_evt_Initialized' ) }}
        WHERE debtTokenName LIKE '%Variable%'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    ) a