{{ config(
        alias = 'pool_trades',
        partition_by = ['day'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}

WITH
  pairs_created AS (
    SELECT
      _nft AS nft_contract_address,
      output_pair AS pair_address
    FROM
      {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }} cre
    WHERE
      call_success
  )

  SELECT
    day,
    CASE
      WHEN trade_category = 'Sell' THEN buyer
      ELSE seller
    END AS pool_address,
    sum(amount_original) AS eth_volume,
    sum(amount_usd) AS usd_volume,
    sum(number_of_items) AS nfts_traded,
    sum(pool_fee_amount) AS owner_fee_volume_eth,
    sum(platform_fee_amount) AS platform_fee_volume_eth,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN -1 * amount_original
        ELSE (amount_original-platform_fee_amount)
      END
    ) AS eth_change_trading,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN number_of_items
        ELSE -1 * number_of_items
      END
    ) AS nft_change_trading
  FROM
    (
      SELECT
        block_date AS day,
        trade_category,
        buyer,
        seller,
        amount_original,
        amount_usd,
        number_of_items,
        pool_fee_amount,
        platform_fee_amount
      FROM
        {{ ref('sudoswap_ethereum_events') }} se
      INNER JOIN pairs_created p ON ((p.nft_contract_address = se.nft_contract_address)
        AND (se.buyer = p.pair_address OR se.seller = p.pair_address))
      {% if not is_incremental() %}
      WHERE se.block_date >= '{{project_start_date}}'
      {% endif %}
      {% if is_incremental() %}
      WHERE se.block_date >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    ) a
  GROUP BY
    1,2
    ;