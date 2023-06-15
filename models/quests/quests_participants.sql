{{
    config(
        alias='participants',
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

SELECT quester_address, block_number, block_time, quest_name, rewards_token as token_address, null as token_id FROM {{ ref('coinbase_wallet_quests_optimism_rewards_transfers') }}
UNION ALL
SELECT quester_address, block_number, block_time, contract_project as quest_name, nft_contract_address as token_address, tokenId as token_id FROM {{ ref('optimism_quests_optimism_quest_completions') }}
UNION ALL
SELECT token_transfer_to as quester_address, block_number, block_time, null as quest_name, nft_contract_address as token_address, tokenId AS token_id FROM {{ ref('galxe_optimism_nft_mints') }}
