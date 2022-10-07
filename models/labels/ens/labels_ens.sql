{{config(alias='ens',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}


-- as default label, take the ENS reverse record or the latest resolver record
SELECT *
FROM (
       SELECT
       array('ethereum') as blockchain,
       coalesce(rev.address, res.address) as address,
       coalesce(rev.name, last(res.name,true) over (order by res.block_time asc)) as name,
       'ENS' as category,
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as modified_at
    FROM {{ ref('ens_resolver_latest') }} res
    FULL OUTER JOIN {{ ref('ens_reverse_latest') }} rev
    ON res.address = rev.address
) ens
UNION
SELECT array('ethereum') as blockchain,
       address,
       name,
       'ENS resolver' as category,
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as modified_at
FROM {{ ref('ens_resolver_latest') }}
UNION
SELECT array('ethereum') as blockchain,
       address,
       name,
       'ENS reverse' as category,
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as modified_at
FROM {{ ref('ens_reverse_latest') }}

