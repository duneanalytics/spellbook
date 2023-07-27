{{ config(
	tags=['legacy'],
	
        alias = alias('art_platform_collections', legacy_model=True)
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
        FROM {{ ref('nft_ethereum_metadata_art_blocks_collections_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_braindrops_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_bright_moments_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_mirage_gallery_curated_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_i_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_ii_legacy') }}
        UNION ALL
        SELECT
                 contract_address,
                 project_id,
                 project_id_base_value,
                 collection_name,
                 artist_name,
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_verse_legacy') }}
)
