{{config(
	tags=['legacy'],
	alias = alias('ens', legacy_model=True),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['blockchain','address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}


-- as default label, take the ENS reverse record or the latest resolver record
SELECT *
FROM (
       SELECT
       'ethereum' as blockchain,
       coalesce(rev.address, res.address) as address,
       coalesce(rev.name, res.name) as name,
       'ENS' as category, --should be social but we can't change this due to how many queries it probably breaks.
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as updated_at,
       "ens" as model_name,
       "identifier" as label_type
    FROM (
        select *
        from (
            select
                address,
                 name
                 ,row_number() over (partition by address order by block_time asc) as ordering
            from {{ ref('ens_resolver_latest_legacy') }}
        ) where ordering = 1
    ) res
    FULL OUTER JOIN {{ ref('ens_reverse_latest_legacy') }} rev
    ON res.address = rev.address
) ens

-- For now, we want to limit the amount of ENS labels to 1
--UNION
--SELECT 'ethereum' as blockchain,
--       address,
--       name,
--       'ENS resolver' as category,
--       '0xRob' as contributor,
--       'query' AS source,
--       date('2022-10-06') as created_at,
--       now() as modified_at
--FROM {{ ref('ens_resolver_latest_legacy') }}
--UNION
--SELECT 'ethereum' as blockchain,
--       address,
--       name,
--       'ENS reverse' as category,
--       '0xRob' as contributor,
--       'query' AS source,
--       date('2022-10-06') as created_at,
--       now() as modified_at
--FROM {{ ref('ens_reverse_latest_legacy') }}

