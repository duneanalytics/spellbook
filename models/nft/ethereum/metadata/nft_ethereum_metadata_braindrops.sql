{{ config(
        alias = 'metadata_braindrops',
        partition_by = ['braindrops_project_id'],
        materialized = 'view',
        unique_key = ['braindrops_project_id']
        )
}}
select contract_address, braindrops_project_id, collection_name, artist_name
from (VALUES
        ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 1, 'Brain Loops', 'Gene Kogan')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 2, 'podGANs', 'Pindar Van Arman')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 3, 'Genesis', 'Claire Silver')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 4, 'DreamScapes', 'Xander Steenbrugge')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 5, 'Confluence', 'Devi Parikh')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 6, '🎵 Fake Feelings', 'Dadabots x Silverstein')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 7, 'Dream Capsules', 'Obvious')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 8, 'Deep Journeys', 'Heavens Last Angel')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 9, 'TEOPEMA', 'Vadim Epstein x COH')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 10, 'SIGHTS', 'Artemis')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 11, 'ClipMatrix Creatures', 'Nikolay Jetchev')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 12, 'Chimerical Stories', 'Entangled Others (Sofia Crespo & Feileacan McCormick)')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 13, 'ELEMENTS', 'ATNPassion')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7 ', 14, 'miniPODs', 'Van Arman x Mindshift x ricky')

) as temp_table (contract_address, braindrops_project_id, collection_name, artist_name)

order by braindrops_project_id asc 
