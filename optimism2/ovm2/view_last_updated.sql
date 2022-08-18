CREATE OR REPLACE VIEW ovm2.view_last_updated AS (

    SELECT
    MAX(block_time) max_time,
    MAX(block_number) AS max_block_number
    
    FROM optimism.transactions WHERE block_time > NOW() - interval '90 days'

) ;
