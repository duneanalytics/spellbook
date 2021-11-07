CREATE TABLE IF NOT EXISTS erc20.stablecoin_evt_transfer (
    contract_address bytea,
    evt_block_number int,
    evt_block_time timestamptz,
    evt_index int,
    evt_tx_hash bytea NOT NULL,
    "from" bytea,
    "to" bytea,
    value numeric
);


BEGIN;

INSERT INTO erc20.stablecoin_evt_transfer (
    contract_address,
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    "from",
    "to",
    value
)

SELECT     
    contract_address,
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    "from",
    "to",
    value
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address in (select contract_address from erc20."stablecoins")


CREATE UNIQUE INDEX "stablecoin_evt_transfer_pkey" ON erc20.stablecoin_evt_transfer USING btree (evt_tx_hash, evt_index)
CREATE INDEX "stablecoin_evt_transfer_contract_address_from_evt_block_time_val_idx" ON erc20.stablecoin_evt_transfer USING btree (contract_address, "from", evt_block_time) INCLUDE (value)
CREATE INDEX "stablecoin_evt_transfer_contract_address_to_evt_block_time_value_idx" ON erc20.stablecoin_evt_transfer USING btree (contract_address, "to", evt_block_time) INCLUDE (value)
CREATE INDEX "stablecoin_evt_transfer_evt_block_number_idx" ON erc20.stablecoin_evt_transfer USING brin (evt_block_number)
CREATE INDEX "stablecoin_evt_transfer_evt_block_time_idx" ON erc20.stablecoin_evt_transfer USING brin (evt_block_time)
CREATE INDEX "stablecoin_evt_transfer_from_contract_address_evt_block_time_val_idx" ON erc20.stablecoin_evt_transfer USING btree ("from", contract_address, evt_block_time) INCLUDE (value)
CREATE INDEX "stablecoin_evt_transfer_from_idx" ON erc20.stablecoin_evt_transfer USING btree ("from")
CREATE INDEX "stablecoin_evt_transfer_to_contract_address_evt_block_time_value_idx" ON erc20.stablecoin_evt_transfer USING btree ("to", contract_address, evt_block_time) INCLUDE (value)


COMMIT;


INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', $$SELECT x.insert_y((SELECT max(block_time) - interval '3 days' FROM x.y));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;