{{ config(
    schema = 'balancer',
    alias = 'single_recipient_gauges'
    )
}}

    --These gauges are deployed by the SingleRecipientGauge contract and this mapping manually links each gauge to it's correspondent pool and project

WITH whitelist_token as (
    SELECT * FROM (values
    (0xb78543e00712c3abba10d0852f6e38fde2aaba4d, 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014, 'veBAL', 'ethereum'),
    (0x56124eb16441a1ef12a4ccaeabdd3421281b795a, 0x9232a548dd9e81bac65500b5e0d918f8ba93675c000200000000000000000423, 'veLIT', 'ethereum'),
    (0x5b79494824bc256cd663648ee1aad251b32693a9, 0xd689abc77b82803f22c49de5c8a0049cc74d11fd000200000000000000000524, 'veUSH', 'ethereum'),
    (0x8e891a7b048a594592e9f0de70dc223143b4f1e6, 0x39eb558131e5ebeb9f76a6cbf6898f6e6dce5e4e0002000000000000000005c8, 'veQi', 'ethereum'),
    (0x24b7aeeefdb612d43f018cbc9c325680f61ec96d, 0x57766212638c425e9cb0c6d6e1683dda369c0fff000200000000000000000678, 'vlGEM', 'ethereum')
    )
        as t (gauge_address, pool_id, project, blockchain))
    
SELECT * FROM whitelist_token