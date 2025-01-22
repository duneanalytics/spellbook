{{config(
    
    alias = 'l2_batch_submitters',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT 'ethereum'                                                                    AS blockchain
     , address
     , COALESCE(protocol_name, ': ', submitter_type, ' - ', version, ' ', role_type) AS name
     , 'infrastructure'                                                              AS category
     , 'msilb7'                                                                      AS contributor
     , 'static'                                                                      AS source
     , TIMESTAMP '2023-06-02'                                                             AS created_at
     , now()                                                                         AS updated_at
     , 'l2_batch_submitters'                                                         AS model_name
     , 'identifier'                                                                  AS label_type

FROM {{ ref('addresses_ethereum_l2_batch_submitters') }}
