{{ config(
        alias = 'bright_moments'
        )
}}


select contract_address, project_id, project_id_base_value, collection_name, artist_name, bright_moments_city, art_collection_unique_id
from (VALUES
        ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 1, 1000000, 'Stellaraum', 'Alida Sun', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-1')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 2, 1000000, 'Parnassus', 'mpkoz', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-2')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 3, 1000000, 'Inflection', 'Jeff Davis', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-3')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 4, 1000000, 'Kaleidoscope', 'Loren Bednar', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-4')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 5, 1000000, 'Lux', 'Jason Ting', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-5')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 6, 1000000, 'Network C', 'Casey REAS', 'Berlin', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-6')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 7, 1000000, 'The Nursery', 'Sputniko', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-7')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 8, 1000000, 'FOLIO', 'Matt DesLauriers', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-8')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 9, 1000000, 'Imprecision', 'Thomas Lin Pedersen', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-9')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 10, 1000000, 'Off Script', 'Emily Xie', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-10')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 11, 1000000, 'Formation', 'Jeff Davis', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-11')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 12, 1000000, 'translucent panes', 'fingacode', 'London', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-12')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 13, 1000000, 'Wirwar', 'Bart Simons', 'Venice', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-13')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 14, 1000000, 'KERNELS', 'Julian Hespenheide', 'All', '0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-14')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 15, 1000000, 'Brise Soleil', 'Jorge Ledezma', 'Venice','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-15')


) as temp_table (contract_address, project_id, project_id_base_value, collection_name, artist_name, bright_moments_city, art_collection_unique_id)
    
order by project_id asc 