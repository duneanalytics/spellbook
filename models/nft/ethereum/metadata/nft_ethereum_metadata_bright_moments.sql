{{ config(
        alias = 'bright_moments',
        partition_by = ['bright_momoments_project_id'],
        materialized = 'view',
        unique_key = ['bright_momoments_project_id']
        )
}}
select contract_address, bright_momoments_project_id, bright_moments_city, collection_name, artist_name
from (VALUES
            ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 1, 'Berlin', 'Stellaraum', 'Alida Sun')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 2, 'Berlin', 'Parnassus', 'mpkoz')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 3, 'Berlin', 'Inflection', 'Jeff Davis')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 4, 'Berlin', 'Kaleidoscope', 'Loren Bednar')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 5, 'Berlin', 'Lux', 'Jason Ting')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 6, 'Berlin', 'Network C', 'Casey REAS')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 7, 'London', 'The Nursery', 'Sputniko')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 8, 'London', 'FOLIO', 'Matt DesLauriers')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 9, 'London', 'Imprecision', 'Thomas Lin Pedersen')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 10, 'London', 'Off Script', 'Emily Xie')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 11, 'London', 'Formation', 'Jeff Davis')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 12, 'London', 'translucent panes', 'fingacode')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 13, 'Venice', 'Wirwar', 'Bart Simons')
            , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 14, 'All', 'KERNELS', 'Julian Hespenheide')

) as temp_table (contract_address, bright_momoments_project_id, bright_moments_city, collection_name, artist_name)

order by bright_momoments_project_id asc 