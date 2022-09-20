{{ config(
        alias = 'mirage_gallery_curated',
        partition_by = ['mirage_gallery_curated_project_id'],
        materialized = 'view',
        unique_key = ['mirage_gallery_curated_project_id']
        )
}}
select contract_address, mirage_gallery_curated_project_id, drop_name, project_name, artist_name
from (VALUES
        ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 1, 'Ebbs and Flows: Our Universe', 'Otherwhere', 'Roope Rainisto & SOMNAI')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 2, 'Ebbs and Flows: Our Universe', 'ANIMA', 'Roope Rainisto & SOMNAI')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 3, 'Life and Death: An Exploration of Impermanence', 'Remnants', 'Austiin')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 4, 'Then and Now: Ever-Changing Worlds', 'Voyage', 'Revrart')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 5, 'Incoherent Elegance', 'Embracing Chaos', 'Saucebook')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 6, 'Latent Travels', 'Yūgen', 'Rikkar')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 7, 'New Dimension', 'Seek', 'Huemin')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 8, 'Entangled Structures', 'MOODs', 'Inner_Sanctum & Pancakes')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 9, 'Artifical Pathways', 'Nexus', 'H01 & DeltaSauce')
        , ('0xb7ec7bbd2d2193b47027247fc666fb342d23c4b5', 10, 'AI Art is Not Art', 'Page', 'Claire Silver')

) as temp_table (contract_address, mirage_gallery_curated_project_id, drop_name, project_name, artist_name)

order by mirage_gallery_curated_project_id asc 

