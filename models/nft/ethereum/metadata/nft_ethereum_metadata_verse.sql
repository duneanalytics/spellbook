{{ config(
        alias = 'verse'
        )
}}


select contract_address, project_id, project_id_base_value, collection_name, artist_name, art_collection_unique_id
from (VALUES
        ('0xbb5471c292065d3b01b2e81e299267221ae9a250', 0, 1000000, 'Hypertype', 'Mark Webster', '0xbb5471c292065d3b01b2e81e299267221ae9a250-0')
        -- double check base value once we have 2nd collection minted 

) as temp_table (contract_address, project_id, project_id_base_value, collection_name, artist_name, art_collection_unique_id)
    
order by project_id asc 