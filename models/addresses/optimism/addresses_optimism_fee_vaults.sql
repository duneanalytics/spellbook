{{config(alias = alias('fee_vaults'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}')}}

-- https://github.com/ethereum-optimism/optimism/blob/develop/op-bindings/predeploys/addresses.go
SELECT  lower(address) as address, vault_name
FROM (VALUES
      ("0x4200000000000000000000000000000000000011","SequencerFeeVault")
     ,("0x4200000000000000000000000000000000000019","BaseFeeVault")
     ,("0x420000000000000000000000000000000000001a","L1FeeVault")
    ) AS x (address, vault_name)
