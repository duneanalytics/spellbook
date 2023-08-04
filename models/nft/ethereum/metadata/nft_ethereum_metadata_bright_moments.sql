{{ config(
        alias = alias('bright_moments')
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
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 16, 1000000, 'Orchids', 'Luke Shannon', 'All','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-16')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 17, 1000000, 'Rubicon', 'Mario Carrillo', 'All','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-17')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 18, 1000000, 'nth culture', 'fingacode', 'London','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-18')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 19, 1000000, 'Pohualli', 'Fahad Karim', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-19')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 20, 1000000, 'Underwater', 'Monica Rizzolli', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-20')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 21, 1000000, 'Infinito', 'Stefano Contiero', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-21')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 22, 1000000, 'Bosque de Chapultepec', 'Daniel Calderon Arenas', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-22')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 23, 1000000, 'ToSolaris', 'Iskra Velitchkova', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-23')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 24, 1000000, 'Glaciations', 'Anna Lucia', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-24')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 25, 1000000, '1935', 'William Mapan', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-25')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 26, 1000000, 'lumen', 'p1xelfool', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-26')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 27, 1000000, 'lo que no esta', 'Marcelo Soria-Rodriguez', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-27')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 28, 1000000, '100 Untitled Spaces', 'Snowfro', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-28')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 29, 1000000, '100 Sunsets', 'Zach Lieberman', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-29')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 30, 1000000, 'Transcendence', 'Jeff Davis', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-30')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 31, 1000000, 'Caminos', 'Juan Rodriguez Garcia', 'Mexico City','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-31')
        , ('0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676', 32, 1000000, 'Color Streams', 'r4v3n', 'All','0x0a1bbd57033f57e7b6743621b79fcb9eb2ce3676-32')


) as temp_table (contract_address, project_id, project_id_base_value, collection_name, artist_name, bright_moments_city, art_collection_unique_id)
    
order by project_id asc 