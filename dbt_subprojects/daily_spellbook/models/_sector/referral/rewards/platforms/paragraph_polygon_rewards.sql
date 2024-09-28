{{ config(
    schema = 'paragraph_polygon',
    alias = 'rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{paragraph_referral_rewards(
        blockchain = "polygon"
        ,FeeManager_evt_FeeDistributed = source('paragraph_polygon','FeeManager_evt_FeeDistributed')
        ,ERC721_call_mintWithReferrer = source('paragraph_polygon','ERC721_call_mintWithReferrer')
        ,ERC721_factory_contract = '0x9e65211e0C29ad7B19FEb5D3D347134629be25Be'
        ,native_currency_contract = '0x0000000000000000000000000000000000001010'
        )
    }}
