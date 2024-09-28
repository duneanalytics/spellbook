{{ config(
    alias = 'tokens'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'atoken_address']
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "the_granary",
                                  \'["msilb7"]\') 
    }}'
  )
}}


SELECT distinct a.blockchain
              , a.atoken_address
              , a.underlying_address
              , a.atoken_decimals
              , a.side
              , a.arate_type
              , a.atoken_symbol
              , a.atoken_name

FROM (
        SELECT 'optimism'        AS blockchain,
               contract_address  AS atoken_address,
               underlyingAsset   AS underlying_address,
               aTokenDecimals    AS atoken_decimals,
               'Supply'          AS side,
               'Variable'        AS arate_type,
               aTokenSymbol      AS atoken_symbol,
               aTokenName        AS atoken_name
        FROM {{source( 'the_granary_optimism', 'AToken_evt_Initialized' ) }}
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

        UNION ALL

        SELECT
            'optimism'          AS blockchain,
            contract_address    AS atoken_address,
            underlyingAsset     AS underlying_address,
            debtTokenDecimals   AS atoken_decimals,
            'Borrow'            AS side,
            CASE WHEN debtTokenName
            LIKE '%Stable%' THEN 'Stable'
            ELSE 'Variable' END AS arate_type,
            debtTokenSymbol     AS atoken_symbol,
            debtTokenName       AS atoken_name
        FROM {{source( 'the_granary_optimism', 'DebtToken_evt_Initialized' ) }}
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        ) a
