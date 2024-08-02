{{
    config(
        alias='completions',
        
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

SELECT 'optimism' as blockchain, 'coinbase wallet quests' as platform, quester_address, block_number, block_time, quest_name, rewards_token as token_address, cast('erc20' as varchar) as token_id FROM {{ ref('coinbase_wallet_quests_optimism_rewards_transfers') }}
    GROUP BY 1,2,3,4,5,6,7,8 -- handle for multiple transfers in one tx
UNION ALL
SELECT 'optimism' as blockchain, 'optimism quests' as platform, quester_address, block_number, block_time, contract_project as quest_name, nft_contract_address as token_address, cast(tokenId as varchar) as token_id FROM {{ ref('optimism_quests_optimism_quest_completions') }}
    GROUP BY 1,2,3,4,5,6,7,8 -- handle for multiple transfers in one tx
UNION ALL
SELECT 'optimism' as blockchain, 'galxe' as platform, token_transfer_to as quester_address, block_number, block_time, null as quest_name, nft_contract_address as token_address, cast(tokenId as varchar) AS token_id FROM {{ ref('galxe_optimism_nft_mints') }}
    GROUP BY 1,2,3,4,5,6,7,8 -- handle for multiple transfers in one tx
