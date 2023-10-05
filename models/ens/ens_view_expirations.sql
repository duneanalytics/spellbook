{{config(alias = alias('view_expirations'),
    post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["antonio-mendes","mewwts"]\') }}')}}
SELECT
    label,
    TO_TIMESTAMP(min(expires)) AS min_expires,
    min(evt_block_time) AS min_evt_block_time,
    TO_TIMESTAMP(max(expires)) AS max_expires,
    max(evt_block_time) AS max_evt_block_time,
    count(*) AS count
FROM (
    SELECT
        conv((id),10,16) AS label,
        expires,
        evt_block_time
    FROM {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRegistered')}}
    UNION
    SELECT
        conv((id),10,16) AS label,
        expires,
        evt_block_time
    FROM {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRenewed')}}
) AS r
GROUP BY label ;
