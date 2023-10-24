{{config(
    
    alias = 'view_expirations',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["antonio-mendes","mewwts"]\') }}')}}
SELECT
    label,
    from_unixtime(min(cast(expires as double))) AS min_expires,
    min(evt_block_time) AS min_evt_block_time,
    from_unixtime(max(cast(expires as double))) AS max_expires,
    max(evt_block_time) AS max_evt_block_time,
    count(*) AS "count"
FROM (
    SELECT
        cast(id as varbinary) AS label,
        expires,
        evt_block_time
    FROM {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRegistered')}}
    UNION
    SELECT
        cast(id as varbinary) AS label,
        expires,
        evt_block_time
    FROM {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRenewed')}}
) AS r
GROUP BY label
