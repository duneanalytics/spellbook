{{config(
    tags=['dunesql']
    , alias = alias('aztec_v2_contracts_ethereum')
)}}

with
  contract_labels as (
    SELECT
      'ethereum' as blockchain,
      contract_address as address,
      description as name,
      contract_type as category,
      'jackiep00' as contributor,
      'wizard' as source,
      date('2022-09-19') as created_at,
      now() as updated_at,
      version,
      protocol,
      'aztec_v2_contracts_ethereum' as model_name,
      'identifier' as label_type
    from
      (
        SELECT
          protocol,
          contract_type,
          version,
          description,
          contract_address
        FROM
          (
            VALUES
              (
                'Aztec RollupProcessor',
                'Rollup',
                '1.0',
                'Prod Aztec Rollup',
                0xFF1F2B4ADb9dF6FC8eAFecDcbF96A2B351680455
              ),
              (
                'Element',
                'Bridge',
                '1.0',
                'Prod Element Bridge',
                0xaeD181779A8AAbD8Ce996949853FEA442C2CDB47
              ),
              (
                'Lido',
                'Bridge',
                '1.0',
                'Prod Lido Bridge',
                0x381abF150B53cc699f0dBBBEF3C5c0D1fA4B3Efd
              ),
              (
                'AceOfZK',
                'Bridge',
                '1.0',
                'Ace Of ZK NFT - nonfunctional',
                0x0eb7f9464060289fe4fddfde2258f518c6347a70
              ),
              (
                'Curve',
                'Bridge',
                '1.0',
                'CurveStEth Bridge',
                0x0031130c56162e00a7e9c01ee4147b11cbac8776
              ),
              (
                'Aztec',
                'Bridge',
                '1.0',
                'Subsidy Manager',
                0xABc30E831B5Cc173A9Ed5941714A7845c909e7fA
              ),
              (
                'Yearn',
                'Bridge',
                '1.0',
                'Yearn Deposits',
                0xE71A50a78CcCff7e20D8349EED295F12f0C8C9eF
              ),
              (
                'Aztec',
                'Bridge',
                '1.0',
                'ERC4626 Tokenized Vault',
                0x3578D6D5e1B4F07A48bb1c958CBfEc135bef7d98
              ),
              (
                'Curve',
                'Bridge',
                '1.0',
                'CurveStEth Bridge V2',
                0xe09801da4c74e62fb42dfc8303a1c1bd68073d1a
              ),
              (
                'Uniswap',
                'Bridge',
                '1.0',
                'UniswapDCABridge',
                0x94679a39679ffe53b53b6a1187aa1c649a101321
              )
              
          ) AS x (
            protocol,
            contract_type,
            version,
            description,
            contract_address
          )
      )
  )
select
  c.*,
  t."from" as contract_creator
from
  contract_labels c
  inner join {{ source('ethereum','traces') }} t on t.type = 'create'
  and c.address = t.address
