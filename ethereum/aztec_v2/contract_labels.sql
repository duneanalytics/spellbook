-- https://dune.com/queries/874721
-- prototype alias table for Aztec V2
drop table if exists aztec_v2.contract_labels cascade;

create table if not exists aztec_v2.contract_labels
(
  protocol varchar,
  contract_type varchar,               
  version varchar,
  description text,
  contract_address bytea                       
);

create index contract_labels_contract_address_idx on aztec_v2.contract_labels(contract_address);
create index contract_labels_protocol_idx on aztec_v2.contract_labels(protocol);

truncate table aztec_v2.contract_labels;

insert into aztec_v2.contract_labels 
(protocol,               contract_type,      version,   description,                   contract_address) values
-- ('Element',              'Bridge',           '0.1',     'Test - Beta bridge',           '\xb5e0Ab45C2c48a6F7032Ee0db749c3c9C5c58A32'::bytea), -- v 0.1 is pre-deployment beta
-- ('Aztec RollupProcessor','Rollup',           '0.1',     'Test - Beta rollup',           '\xff6bed1e4d28491b89a02dc56b34a4b273eb9e0d'::bytea),
-- ('Lido',                 'Bridge',           '0.1',     'Test - Beta bridge',           '\xFDb2f2E720436972644bf824dEBea47F07C5041D'::bytea),
('Aztec RollupProcessor','Rollup',           '1.0',     'Prod Aztec Rollup',            '\xFF1F2B4ADb9dF6FC8eAFecDcbF96A2B351680455'::bytea),
('Element',              'Bridge',           '1.0',     'Prod Element Bridge',          '\xaeD181779A8AAbD8Ce996949853FEA442C2CDB47'::bytea),
('Lido',                 'Bridge',           '1.0',     'Prod Lido Bridge',             '\x381abF150B53cc699f0dBBBEF3C5c0D1fA4B3Efd'::bytea),
('AceOfZK',              'Bridge',           '1.0',     'Ace Of ZK NFT - nonfunctional','\x0eb7f9464060289fe4fddfde2258f518c6347a70'::bytea),
('Curve',                'Bridge',           '1.0',     'CurveStEth Bridge',            '\x0031130c56162e00a7e9c01ee4147b11cbac8776'::bytea)
;

alter table aztec_v2.contract_labels add contract_creator bytea;

update aztec_v2.contract_labels
set contract_creator = t."from"
from ethereum.traces t
where t.type = 'create'
and contract_labels.contract_address = t.address;