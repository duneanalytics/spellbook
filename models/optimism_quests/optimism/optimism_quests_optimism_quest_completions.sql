{{ config(
    alias = alias('quest_completions'),
    tags=['dunesql'],
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'nft_id'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "optimism_quests",
                                \'["msilb7"]\') }}'
    )
}}
--SELECT MIN(call_block_time) FROM optimism_quest_optimism.StarNFTV4_call_mint m
{% set project_start_date = '2022-09-13' %}

SELECT
    cast( DATE_TRUNC('day',call_block_time) as date) AS block_date,
    cast( DATE_TRUNC('month',call_block_time) as date) AS block_month,
    account AS quester_address,
    call_tx_hash AS tx_hash,
    call_block_number AS block_number,
    call_block_time AS block_time,
    nft_id,
    contract_project,
    quest_project,
    contract_address AS nft_contract_address,
    output_0 as tokenId

FROM
    {{source('optimism_quest_optimism','StarNFTV4_call_mint')}} m
INNER JOIN {{ref('optimism_quests_optimism_nft_id_mapping')}} nft 
    ON cast(m.cid as varchar) = nft.nft_id

WHERE call_success = true
AND call_block_time >= timestamp '{{project_start_date}}'
{% if is_incremental() %}
AND call_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}