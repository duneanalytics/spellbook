{{ config(
        tags = ['static']
        ,schema = 'nft_ethereum_metadata'
        ,alias = 'bryan_brinkman_collections'
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "nft_ethereum_metadata",
                                    contributors = \'["rickmanelius"]\') }}'
        )
}}

select *
        , date(mint_date) as mint_date_date
        , date_format(date(mint_date),'%b-%d-%Y') as mint_date_date_format

from 
(
select title, art_type, platform, edition_count, blockchain, token_standard, contract_address, min_token_id, max_token_id, category, link, mint_date
from (VALUES
        ('NIMBUDS', 'Generative', 'Art Blocks', 400, 'Ethereum', 'erc721', 0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270, 10000000, 10000399, '', 'https://opensea.io/collection/nimbuds-by-bryan-brinkman', '2021-01-16')
) as temp_table (title, art_type, platform, edition_count, blockchain, token_standard, contract_address, min_token_id, max_token_id, category, link, mint_date)
) a 