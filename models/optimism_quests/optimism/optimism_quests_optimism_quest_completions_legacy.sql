{{ config(
	tags=['legacy'],
	
    alias = alias('quest_completions', legacy_model=True),
    partition_by = ['block_date'],
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
    DATE_TRUNC('day',call_block_time) AS block_date,
    account AS quester_address,
    call_tx_hash AS tx_hash,
    call_block_number AS block_number,
    call_block_time AS block_time,
    nft_id,
    contract_project,
    quest_project

FROM
    {{source('optimism_quest_optimism','StarNFTV4_call_mint')}} m
INNER JOIN {{ref('optimism_quests_optimism_nft_id_mapping_legacy')}} nft 
    ON cast(m.cid as varchar(4)) = nft.nft_id

WHERE call_success = true
AND call_block_time >= cast( '{{project_start_date}}' as timestamp)
{% if is_incremental() %}
AND call_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}