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
from query_3961807 a -- full list 
full outer join query_3965956 b
on a.title = b.title and a.art_type = b.art_type