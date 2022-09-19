{{config(alias='aztec_v2_contract_labels')}}

with contract_labels as (
    SELECT array('ethereum') as blockchain,
        contract_address as address, 
        description as name,
        contract_type as category,
        'jackiep00' as contributor,
        'wizard' as source,
        date('2022-09-19') as created_at,
        now() as updated_at,
        version, 
        protocol
    from
    (SELECT protocol, contract_type, version, description, contract_address
    FROM (VALUES
        ('Aztec RollupProcessor','Rollup',           '1.0',     'Prod Aztec Rollup',            '\xFF1F2B4ADb9dF6FC8eAFecDcbF96A2B351680455'::bytea),
        ('Element',              'Bridge',           '1.0',     'Prod Element Bridge',          '\xaeD181779A8AAbD8Ce996949853FEA442C2CDB47'::bytea),
        ('Lido',                 'Bridge',           '1.0',     'Prod Lido Bridge',             '\x381abF150B53cc699f0dBBBEF3C5c0D1fA4B3Efd'::bytea),
        ('AceOfZK',              'Bridge',           '1.0',     'Ace Of ZK NFT - nonfunctional','\x0eb7f9464060289fe4fddfde2258f518c6347a70'::bytea),
        ('Curve',                'Bridge',           '1.0',     'CurveStEth Bridge',            '\x0031130c56162e00a7e9c01ee4147b11cbac8776'::bytea),
        ('Aztec',                'Bridge',           '1.0',     'Subsidy Manager',              '\xABc30E831B5Cc173A9Ed5941714A7845c909e7fA'::bytea),
        ('Yearn',                'Bridge',           '1.0',     'Yearn Deposits',               '\xE71A50a78CcCff7e20D8349EED295F12f0C8C9eF'::bytea),
        ('Aztec',                'Bridge',           '1.0',     'ERC4626 Tokenized Vault',      '\x3578D6D5e1B4F07A48bb1c958CBfEc135bef7d98'::bytea))
        AS x (protocol, contract_type, version, description, contract_address))
)
select c.*, t."from" as contract_creator
from contract_labels c
inner join ethereum.traces t
where t.type = 'create'
and c.contract_address = t.address;
