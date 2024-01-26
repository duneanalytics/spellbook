{{ config(
    schema = 'paragraph_base',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{paragraph_referral_rewards(
        blockchain = "base"
        ,FeeManager_evt_FeeDistributed = source('paragraph_base','FeeManager_evt_FeeDistributed')
        ,ERC721_call_mintWithReferrer = source('paragraph_base','ERC721_call_mintWithReferrer')
        ,ERC721_factory_contract = '0x3E3255CbE27f34A981A8AFA98192e77Eb198901A'
        )
    }}
