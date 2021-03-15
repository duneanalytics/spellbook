CREATE TABLE Superrare.superrare_full_activity_list5 (
    tx_hash bytea,
    block_time timestamptz NOT NULL,
    token_id VARCHAR,
    "from" VARCHAR,
    "to" VARCHAR,
    amount int,
    price int,
    category VARCHAR,
    gas_fee int,
    include VARCHAR
);

CREATE OR REPLACE FUNCTION Superrare.create_full_activity_list(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN



WITH all_activities AS ( --https://etherscan.io/tx/0xa2c07597bb4350d1084e78c62059d30f2179d58eade0cb0612da4ffb0251d2bb
    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic2 FROM 13) as VARCHAR) AS "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        0 AS amount,
        'Token created' AS category
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic2 = '\x0000000000000000000000000000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(data) as VARCHAR) AS token_id,
        CAST(substring(topic2 FROM 13) as VARCHAR) AS "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        0 AS amount,
        'Token created' AS category
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic2 = '\x0000000000000000000000000000000000000000000000000000000000000000'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic4 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'
    UNION ALL
    SELECT   -- https://etherscan.io/tx/0x1acb61634e16bbbc94524dcc523ccd15137e6fd97a28993126354dce146cd310
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic2 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\x16dd16959a056953a63cf14bf427881e762e54f03d86b864efea8238dd3b822f'
    UNION ALL
    SELECT   -- https://etherscan.io/tx/0x0ea7893c43530ab7e5946a17236fc53730e3d11e10c9922e05ac2d1b15bebf92
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic4 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'

UNION ALL


    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST('\xEscrow' as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Offer' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\xd21fbaad97462831ad0c216f300fefb33a10b03bb18bb70ed668562e88d15d53'
    UNION ALL
    SELECT   -- https://etherscan.io/tx/0x81cdd46ecad7f3516ca230c43ee40874693f86cf751b254c7fd9a228dfbd600c
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic2 FROM 13) as VARCHAR) as "from",
        CAST('\xEscrow' as VARCHAR) as "to",
        bytea2numericpy(topic3) / 10^18 AS amount,
        'Offer' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\x19421268847f42dd61705778018ddfc43bcdce8517e7a630acb12f122c709481'
    UNION ALL
    SELECT   --- https://etherscan.io/tx/0x2f84705a848db9c76392297a09af8d310100b5f15ca955f6cbdfe8d028403814#eventlog
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST('\xEscrow' as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Offer' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\xd21fbaad97462831ad0c216f300fefb33a10b03bb18bb70ed668562e88d15d53'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic4 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Offer accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'
    UNION ALL
    SELECT   -- https://etherscan.io/tx/0x548d6a9d3b64e8012578435fc84f4b1f18e8ab2759a5d4a1d8d5fdbfc5b4e828
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic2 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Offer accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\xd6deddb2e105b46d4644d24aac8c58493a0f107e7973b2fe8d8fa7931a2912be'
    UNION ALL
    SELECT    -- https://etherscan.io/tx/0xeae8d230cd5ce305f6af3f7a5ce00586560092124c80a8c90f086ac9fc6c343c
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic4 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Offer accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic3) as VARCHAR) AS token_id,
        CAST(substring(topic4 FROM 13) as VARCHAR) as "from",
        CAST(substring(topic4 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Launch reserve auction' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\x51d3615735f04b7ff33bfd9f5eb857d899bf9b848d3858cba7db855bc69fb914'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic3) as VARCHAR) AS token_id,
        CAST(substring(topic4 FROM 13) as VARCHAR) as "from",
        CAST('\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656' as VARCHAR) as "to",
        bytea2numericpy(substring(data FROM 33 FOR 32)) / 10^18 AS amount,
        'Launch timed auction' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\x97ef537a4a14d8899c80db7b0665dac266da778443e37e073ec2a11ec62bea5b'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST('\xEscrow' as VARCHAR) as "to",   -- escrow address
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Bid' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\x5d22b2d23515fb6c26c46ebec88f4a8b503493be518661c852adf894344cbae7'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST(substring(topic3 FROM 13) as VARCHAR) as "from",
        CAST(substring(data FROM 13 FOR 20) as VARCHAR) as "to", 
        bytea2numericpy(substring(data FROM 33 FOR 32)) / 10^18 AS amount,
        CASE WHEN topic3 = '\x0000000000000000000000000000000000000000000000000000000000000000' THEN 'Auction retired' ELSE 'Auction settled' END AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9'

UNION ALL

    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST('\xEscrow' as VARCHAR) as "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Withdraw' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\x99a3761c98d7a0c3980cbeb3d8009b315a463f8020b43ca1e6901611b06547f9' 
    UNION ALL
    SELECT   -- https://etherscan.io/tx/0x5ba4256d0331486e834a148ab5b4da5121b173f8c5ac13e8d70c4eb0e651bf55
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
        CAST('\xEscrow' as VARCHAR) as "from",
        CAST(substring(topic2 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(topic3) / 10^18 AS amount,
        'Withdraw' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\x09dcebe16a733e22cc47e4959c50d4f21624d9f1815db32c2e439fbbd7b3eda0'
    UNION ALL
    SELECT
        tx_hash,
        date_trunc('minute', block_time) AS time,
        CAST(bytea2numericpy(substring(data FROM 33)) as VARCHAR) AS token_id,
        CAST('\xEscrow' as VARCHAR) as "from",
        CAST(substring(topic3 FROM 13) as VARCHAR) as "to",
        bytea2numericpy(substring(data FOR 32)) / 10^18 AS amount,
        'Withdraw' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\x99a3761c98d7a0c3980cbeb3d8009b315a463f8020b43ca1e6901611b06547f9' 

UNION ALL

    SELECT 
        tx_hash,
        date_trunc('minute', ethereum.logs.block_time) as time,
        CAST(bytea2numericpy(substring(ethereum.logs.data FROM 33)) as VARCHAR) as token_id,
        CAST("from" as VARCHAR) as "from",
        CAST("to" AS VARCHAR) as "to",
        bytea2numericpy(substring(ethereum.logs.data FOR 32)) / 10^18 as amount,
        'List price updated' as category
    FROM ethereum.logs
    LEFT JOIN ethereum.transactions tx ON hash = tx_hash
    WHERE contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND topic1 = '\xb0b0e4adf2724af8f1646eae3a16f45d696c9334594729d09bf192da1f783871'
    UNION ALL
    SELECT  -- https://etherscan.io/tx/0x86873ca09f5d1a884c7c0ea6aa1cb57ff203740f78cf3620c7998a55e5ecb6c8
        tx_hash,
        date_trunc('minute', ethereum.logs.block_time) as time,
        CAST(bytea2numericpy(substring(ethereum.logs.data FROM 33)) as VARCHAR) as token_id,
        CAST("from" as VARCHAR) as "from",
        CAST("to" AS VARCHAR) as "to",
        bytea2numericpy(substring(ethereum.logs.data FOR 32)) / 10^18 as amount,
        'List price updated' as category
    FROM ethereum.logs
    LEFT JOIN ethereum.transactions tx ON hash = tx_hash
    WHERE contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND topic1 = '\xb0b0e4adf2724af8f1646eae3a16f45d696c9334594729d09bf192da1f783871'


),

token_burning as  
(
SELECT
CAST(bytea2numericpy(topic4) as VARCHAR) AS token_id,
'BURNED' AS include
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic3 = '\x00000000000000000000000041a322b28d0ff354040e2cbc676f0320d8c8850d'
    UNION ALL
    SELECT    -- https://etherscan.io/tx/0x3d8b5b7bf921c2608aa02c2743c07b39155d5156ff4971701aba76e5b1879452
        CAST(bytea2numericpy(data) as VARCHAR) AS token_id,
        'BURNED' AS include
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic3 = '\x00000000000000000000000041a322b28d0ff354040e2cbc676f0320d8c8850d'
),

rows AS (
    INSERT INTO Superrare.superrare_full_activity_list5 (
    tx_hash,
    block_time,
    token_id,
    "from",
    "to",
    amount,
    price,
    category,
    gas_fee,
    include
    )
    SELECT
    tx_hash,
    a.time, 
    a.token_id,
    a."from",
    a."to",
    amount,
    amount*price as price,
    category,
    (((gas_used * gas_price) / 10^18) * price) as gas_fee, 
    include
    FROM 
    all_activities a
    LEFT JOIN token_burning ON token_burning.token_id = a.token_id 
    LEFT JOIN (SELECT * FROM prices."layer1_usd" WHERE symbol = 'ETH' AND minute > '2019-01-01') price ON minute = time
    LEFT JOIN ethereum."transactions" ON hash = tx_hash
    WHERE symbol = 'ETH' --AND include IS NULL
    ORDER BY time DESC
    ON CONFLICT DO NOTHING
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


SELECT Superrare.create_full_activity_list('2021-03-14', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-03-14'), (SELECT MAX(number) FROM ethereum.blocks)) WHERE NOT EXISTS (SELECT * FROM Superrare.superrare_full_activity_list5 LIMIT 1)
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT Superrare.create_full_activity_list((SELECT max(block_time) - interval '2 days' FROM Superrare.superrare_full_activity_list5), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM Superrare.superrare_full_activity_list5)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;








   

