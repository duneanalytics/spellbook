{{
    config(
        schema = 'nft_scroll',
        alias = 'approvals',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash', 'evt_index']
    )
}}

{{
    nft_approvals(
        blockchain = 'scroll',
        erc721_approval = source('erc721_scroll', 'evt_Approval'),
        erc721_approval_all = source('erc721_scroll', 'evt_ApprovalForAll'),
        erc1155_approval_all = source('erc1155_scroll', 'evt_ApprovalForAll')
    )
}}
