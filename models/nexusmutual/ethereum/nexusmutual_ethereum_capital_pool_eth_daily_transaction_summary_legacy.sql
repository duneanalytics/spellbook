{{ config(
	tags=['legacy'],
	
    alias = alias('capital_pool_eth_daily_transaction_summary', legacy_model=True),
    partition_by = ['day'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = ['day'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["guyhowlett"]\') }}'
    )
}}

{% set project_start_date = '2019-05-12' %}

WITH
  ethereum_transactions AS (
    SELECT
      block_time,
      DATE_TRUNC('day', block_time) AS day,
      t.to,
      t.from,
      value AS eth_value
    FROM
      {{ source('ethereum', 'traces') }} t
    WHERE
      success
{% if not is_incremental() %}
       AND block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
      AND block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
      AND (
        t.to IN (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfd61352232157815cf7b71045557192bf0ce1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          '0xcafea112db32436c2390f5ec988f3adb96870627'
        )
        OR t.from IN (
          '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
          '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
          '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
          '0xcafea8321b5109d22c53ac019d7a449c947701fb',
          '0xfd61352232157815cf7b71045557192bf0ce1884',
          '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
          '0xcafea112db32436c2390f5ec988f3adb96870627'
        )
      )
      AND NOT (
        (
          t.to = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
          AND t.from = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        )
        OR (
          t.to = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND t.from = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        )
        OR (
          t.to = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND t.from = '0xfd61352232157815cf7b71045557192bf0ce1884'
        )
      )
  )
SELECT DISTINCT
  day,
  SUM(
    CASE
      WHEN t.to IN (
        '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
        '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
        '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
        '0xcafea8321b5109d22c53ac019d7a449c947701fb',
        '0xfd61352232157815cf7b71045557192bf0ce1884',
        '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
        lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
      ) THEN eth_value * 1E-18
      ELSE 0
    END
  ) OVER (
    PARTITION BY
      day
  ) AS eth_ingress,
  SUM(
    CASE
      WHEN t.from IN (
        '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
        '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
        '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
        '0xcafea8321b5109d22c53ac019d7a449c947701fb',
        '0xfd61352232157815cf7b71045557192bf0ce1884',
        '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
        lower('0xcafea112Db32436c2390F5EC988f3aDB96870627')
      ) THEN eth_value * 1E-18
      ELSE 0
    END
  ) OVER (
    PARTITION BY
      day
  ) AS eth_egress
FROM
  ethereum_transactions t