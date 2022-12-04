{{ config(
    schema = 'the_granary'
    , alias='tokens'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'atoken_address']
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "the_granary",
                                  \'["msilb7"]\') }}'
  )
}}


WITH atokens AS (
    SELECT distinct 
        blockchain
            , a.atoken_address, a.underlying_address, a.atoken_decimals, a.side, a.arate_type, a.atoken_symbol, a.atoken_name
            , et.decimals AS underlying_decimals, et.symbol AS underlying_symbol
    
    FROM (
        SELECT
        'optimism' AS blockchain, contract_address as atoken_address, `underlyingAsset` as underlying_address, `aTokenDecimals` as atoken_decimals, 'Supply' as side, 'Variable' as arate_type, aTokenSymbol as atoken_symbol, aTokenName as atoken_name
            FROM {{source( 'the_granary_optimism', 'AToken_evt_Initialized' ) }}
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        UNION ALL
        SELECT
        'optimism' AS blockchain, contract_address as atoken_address, `underlyingAsset` as underlying_address, `debtTokenDecimals` as atoken_decimals, 'Borrow' as side, 'Stable' as arate_type, debtTokenSymbol as atoken_symbol, debtTokenName as atoken_name
            FROM {{source( 'the_granary_optimism', 'StableDebtToken_evt_Initialized' ) }}
            WHERE debtTokenName LIKE '%Stable%'
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        UNION ALL
        SELECT
        'optimism' AS blockchain, contract_address as atoken_address, `underlyingAsset` as underlying_address, `debtTokenDecimals` as atoken_decimals, 'Borrow' as side, 'Variable' as arate_type, debtTokenSymbol as atoken_symbol, debtTokenName as atoken_name
            FROM {{source( 'the_granary_optimism', 'VariableDebtToken_evt_Initialized' ) }}
            WHERE debtTokenName LIKE '%Variable%'
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) a
    LEFT JOIN {{ ref('tokens_erc20') }} et
        ON a.underlying_address = et.contract_address
        AND et.blockchain = 'optimism'

    )