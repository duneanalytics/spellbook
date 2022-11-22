CREATE OR REPLACE VIEW ethereumnameservice.view_expirations AS
SELECT
    label,
    TO_TIMESTAMP(min(expires)) AS min_expires,
    min(evt_block_time) AS min_evt_block_time,
    TO_TIMESTAMP(max(expires)) AS max_expires,
    max(evt_block_time) AS max_evt_block_time,
    count(*) AS count
FROM (
    SELECT
        numeric2bytea(id) AS label,
        expires,
        evt_block_time
    FROM
        ethereumnameservice. "BaseRegistrarImplementation_evt_NameRegistered"
    UNION
    SELECT
        numeric2bytea(id) AS label,
        expires,
        evt_block_time
    FROM
        ethereumnameservice. "BaseRegistrarImplementation_evt_NameRenewed") AS r
GROUP BY
    label;
