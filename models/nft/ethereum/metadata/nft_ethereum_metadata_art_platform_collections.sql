{{ config(
        tags = ['static','dunesql']
        ,schema = 'nft_ethereum_metadata'
        ,alias = alias('art_platform_collections')
)
}}

SELECT *
FROM
(
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_art_blocks_collections') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_braindrops') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_bright_moments') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_mirage_gallery_curated') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_i') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_ii') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_verse') }}
)
