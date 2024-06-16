{{ config(
        
        schema = 'limitbreak_ethereum',
        alias = 'creator_tokens',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['address']
        )
}}

{{creator_tokens_inspect_contracts(
    blockchain='ethereum'
)}}
