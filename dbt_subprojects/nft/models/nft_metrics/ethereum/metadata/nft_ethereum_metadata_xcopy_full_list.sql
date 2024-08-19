{{ config(
        tags = ['static']
        ,schema = 'nft_ethereum_metadata'
        ,alias = 'xcopy_full_list'
        ,materialized = 'table'
        )
}}

select a.title
        , a.art_type
        , platform
        , edition_count
        , blockchain
        , token_standard
        , contract_address
        , min_token_id
        , max_token_id
        , nonsequential_token_id
        , clean_token_id
        , category
        , link
        , mint_date_date_format
from {{ ref('nft_ethereum_metadata_xcopy_collections') }} a
full outer join {{ ref('nft_ethereum_metadata_xcopy_nonsequential_tokens') }} b
on a.title = b.title and a.art_type = b.art_type