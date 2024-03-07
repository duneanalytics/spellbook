{{ config(
    schema = 'paragraph_zora',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{paragraph_referral_rewards(
        blockchain = "zora"
        ,FeeManager_evt_FeeDistributed = source('paragraph_zora','FeeManager_evt_FeeDistributed')
        ,ERC721_call_mintWithReferrer = source('paragraph_zora','ERC721_call_mintWithReferrer')
        ,ERC721_factory_contract = '0x81854B78c51BFff1d87A350e99C331161d423cF1'
        )
    }}
