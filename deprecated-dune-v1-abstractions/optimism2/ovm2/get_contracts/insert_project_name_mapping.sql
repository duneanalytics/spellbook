--Fix project display names where the dune decoding doesn't match up (i.e. Perp vs Perp v2, Lyra vs Lyra v1)

CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.project_name_mappings (
  dune_name text,
  mapped_name text,
        UNIQUE (dune_name, mapped_name)
);


BEGIN;
DELETE FROM ovm2.project_name_mappings *;

COPY ovm2.project_name_mappings (dune_name,mapped_name) FROM stdin;
lyra_v1	Lyra
Lyra V1	Lyra
aave_v3	Aave
perp_v2	Perpetual Protocol
synthetix_futures	Kwenta
zeroex	0x
uniswap_v3	Uniswap V3
Uniswap V3	Uniswap V3
oneinch	1inch
pika_perp_v2	Pika Protocol
quixotic_v1	Quixotic
quixotic_v2	Quixotic
quixotic_v3	Quixotic
quixotic_v4	Quixotic
across_v2	Across
openocean_v2	OpenOcean
setprotocol_v2	Set Protocol
kromatikafinance	Kromatika
kratosdao	Kratos Dao
curvefi	Curve
pika_perp	Pika Protocol
dhedge_v2	Dhedge
bitbtc	Bitbtc Protocol
teleportr	Teleportr/ Warp Speed
balancer_v2	Beethoven X
stargate	Stargate Finance
quixotic_v5	Quixotic
lyra_avalon	Lyra
Lyra Avalon	Lyra
Unlock	Unlock Protocol
Xy Finance	XY Finance
Qidao	QiDao
Defisaver	Defi Saver
Layerzero	Layer Zero
Xtoken	xToken
Instadapp	InstaDapp
Lifi	LiFi
Optimistic Exporer	Optimistic Explorer - Get Started NFT
ironbank	Iron Bank
iron_bank	Iron Bank
bluesweep	BlueSweep
hidden_hand	Hidden Hand
\.

COMMIT;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dune_name_mapped_name_uniq_idx ON ovm2.project_name_mappings (dune_name);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dune_name_mapped_name_low_uniq_idx ON ovm2.project_name_mappings (LOWER(dune_name));
