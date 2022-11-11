{{ config(
        alias ='art_platform_collections'
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
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_braindrops') }}
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_bright_moments') }}
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_mirage_gallery_curated') }}        
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_i') }}              
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_proof_grails_ii') }}    
        UNION
        SELECT
                 contract_address, 
                 project_id, 
                 project_id_base_value, 
                 collection_name, 
                 artist_name, 
                 art_collection_unique_id
        FROM {{ ref('nft_ethereum_metadata_verse') }}    
)