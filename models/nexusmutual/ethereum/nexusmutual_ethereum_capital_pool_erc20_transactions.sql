
{{ config(
    alias ='capital_pool_erc20_transactions',
    partition_by = ['block_time'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = ['day'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["guyeth"]\') }}'
    )
}}

{% set project_start_date = '2019-05-12' %}

SELECT
  evt_block_time as block_time,
  CASE
    WHEN a.contract_address = '0x27f23c710dd3d878fe9393d93465fed1302f2ebd' THEN 'nxmty'
    ELSE name
  END AS name,
  a.contract_address as contract_address,
  a.to,
  a.from,
  CASE
    WHEN a.to IN (
      '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
      '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
      '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
      '0xcafea8321b5109d22c53ac019d7a449c947701fb',
      '0xfd61352232157815cf7b71045557192bf0ce1884',
      '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
      '0xcafea112db32436c2390f5ec988f3adb96870627'
    ) THEN CAST(value AS DOUBLE) * 1E-18
    ELSE 0
  END AS ingress,
  CASE
    WHEN a.from IN (
      '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
      '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
      '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
      '0xcafea8321b5109d22c53ac019d7a449c947701fb',
      '0xfd61352232157815cf7b71045557192bf0ce1884',
      '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
      '0xcafea112db32436c2390f5ec988f3adb96870627'
    ) THEN CAST(value AS DOUBLE) * 1E-18
    ELSE 0
  END AS egress
  FROM
  {{ source('erc20_ethereum', 'evt_transfer') }} as a
  LEFT JOIN {{ ref('labels_contracts') }} AS b ON a.contract_address = b.address
{% if not is_incremental() %}
  WHERE evt_block_time >= '{{project_start_date }}'
{% endif %}
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
  AND
  (
    a.to IN (
      '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
      '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
      '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
      '0xcafea8321b5109d22c53ac019d7a449c947701fb',
      '0xfd61352232157815cf7b71045557192bf0ce1884',
      '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
      '0xcafea112db32436c2390f5ec988f3adb96870627'
    )
    OR a.from IN (
      '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8',
      '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb',
      '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b',
      '0xcafea8321b5109d22c53ac019d7a449c947701fb',
      '0xfd61352232157815cf7b71045557192bf0ce1884',
      '0x7cbe5682be6b648cc1100c76d4f6c96997f753d6',
      '0xcafea112db32436c2390f5ec988f3adb96870627'
    )
  )
  AND (
    name IN ('Maker: dai', 'Lido: steth')
    OR a.contract_address = '0x27f23c710dd3d878fe9393d93465fed1302f2ebd' /* nxmty */
  )
  AND NOT (
    (
      a.to = '0xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
      AND a.from = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
    )
    OR (
      a.to = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
      AND a.from = '0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
    )
    OR (
      a.to = '0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
      AND a.from = '0xfd61352232157815cf7b71045557192bf0ce1884'
    )
  )