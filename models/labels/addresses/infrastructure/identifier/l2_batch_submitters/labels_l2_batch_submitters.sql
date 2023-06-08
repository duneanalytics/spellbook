{{config(
    alias='l2_batch_submitters',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT
  'ethereum' as blockchain, address, COALESCE(protocol_name, ': ', submitter_type, ' - ', version,' ',role_type) AS name
  , 'infrastructure' AS category, 'msilb7' as contributor, 'static' as source, timestamp('2023-06-02') as created_at
, now() AS updated_at, 'l2_batch_submitters' as model_name, 'identifier' as label_type

FROM {{ ref('addresses_ethereum_l2_batch_submitters') }}
