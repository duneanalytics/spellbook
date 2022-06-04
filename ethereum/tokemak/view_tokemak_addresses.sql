CREATE MATERIALIZED VIEW tokemak.view_tokemak_addresses 
(
	address
) AS (

    SELECT '\x9e0bcE7ec474B481492610eB9dd5D69EB03718D5' ::bytea AS address /*deployer*/
    UNION 
    SELECT '\x90b6C61B102eA260131aB48377E143D6EB3A9d4B' ::bytea AS address/*coordinator*/
    UNION 
    SELECT '\xA86e412109f77c45a3BC1c5870b880492Fb86A14' ::bytea AS address/*manager*/
    UNION 
    SELECT '\x8b4334d4812c530574bd4f2763fcd22de94a969b' ::bytea as address /*treasury*/

);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_addresses (
   address
);

INSERT INTO cron.job(schedule, command)
VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_addresses$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;