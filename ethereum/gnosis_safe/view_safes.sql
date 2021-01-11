BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes;
create materialized view gnosis_safe.view_safes as
    with safe_setups as (
        select call_tx_hash as tx_hash, _threshold as threshold, array_length(_owners, 1) as owners
        from gnosis_safe."Safev0.1.0_call_setup"
        union all
        select call_tx_hash as tx_hash, _threshold as threshold, array_length(_owners, 1) as owners
        from gnosis_safe."Safev1.0.0_call_setup"
        union ALL
        select call_tx_hash as tx_hash, _threshold as threshold, array_length(_owners, 1) as owners
        from gnosis_safe."Safev1.1.0_call_setup"
        union ALL
        select call_tx_hash as tx_hash, _threshold as threshold, array_length(_owners, 1) as owners
        from gnosis_safe."Safev1.1.1_call_setup"
    ),
    safes as (
        select
            et.block_number as creation_block_number,
            et.block_time as creation_time,
            et.from as address,
            -- mastercopy v0.1.0:
            --     when 3 or 4 owners
            --         and threshold 1 or 2
            --         and no call to proxy factory then personal edition, else unknown
            -- mastercopy > v0.1.0 -> We started using ProxyFactory and create2, also Safe for Teams was released
            --     When Gnosis relayer triggered ProxyFactory via createProxyWithNonce
            --         and 3 or 4 owners
            --         and threshold 1 or 2 then Personal Safe
            --     When ProxyFactory was triggered via createProxy by something other than the Gnosis relayer then we consider it a Team Safe
            --     When something else, then unknown (e.g. no factory used or createProxyWithNonce by something other than relay service
            --        If unknown is increasing, we could drill down more.
            case
                when
                    (owners = 3 or owners = 4)
                    and (threshold = 1 or threshold = 2)
                    and (
                        (et.to = '\x8942595A2dC5181Df0465AF0D7be08c8f23C93af'  -- master copy address v0.1.0
                            and (et2.to is null or et2.to <> '\x88cd603a5dc47857d02865bbc7941b588c533263')
                        )
                        or (et.to in ('\xb6029ea3b2c51d09a50b53ca8012feeb05bda35a', '\xae32496491b53841efb51829d6f886387708f99b', '\x34cfac646f301356faa8b21e94227e3583fe3f5f') -- mastercopy address v1.0.0, v1.1.0, v1.1.0, v1.1.1
                            and et2.from = '\x07f455f30e862e13e3e3d960762cb11c4f744d52'  -- relay service
                            and et2.to in ('\x12302fe9c02ff50939baaaaf415fc226c078613c', '\x76e2cfc1f5fa8f6a5b3fc4c8f4788f0116861f9b')  -- ProxyFactory v1.0.0, v1.1.1
                            and substring(et2."input" for 4) = '\x1688f0b9' -- createProxyWithNonce method
                        )
                    )
                then 'personal'
                when
                    et.to in ('\xb6029ea3b2c51d09a50b53ca8012feeb05bda35a', '\xae32496491b53841efb51829d6f886387708f99b', '\x34cfac646f301356faa8b21e94227e3583fe3f5f') -- mastercopy address v1.0.0, v1.1.0, v1.1.1
                    and et2.from <> '\x07f455f30e862e13e3e3d960762cb11c4f744d52'  -- relay service
                    and et2.to in ('\x12302fe9c02ff50939baaaaf415fc226c078613c', '\x76e2cfc1f5fa8f6a5b3fc4c8f4788f0116861f9b')  -- ProxyFactory v1.0.0, v1.1.1
                    and substring(et2."input" for 4) in ('\x61b69abd', '\x1688f0') -- createProxy method
                then 'team'
                else 'unknown'
                end
             as safe_type
        from ethereum.traces et -- for the setup calls
        left outer join safe_setups ss
            on et.tx_hash = ss.tx_hash
        left outer join ethereum.traces et2  -- for the potential proxy calls
            on et.tx_hash = et2.tx_hash and et2.call_type = 'call' and substring(et2."input" for 4) in ('\x1688f0b9', '\x61b69abd') -- createProxyWithNonce or createProxy method call
        where et.tx_success = True
            AND substring(et."input" for 4) in ('\x0ec78d9e', '\xa97ab18a', '\xb63e800d') -- setup methods of v0.1.0, v1.0.0, v1.1.0=v.1.1.1
            AND et.call_type = 'delegatecall' -- the delegate call to the master copy is the Safe address
            AND et.to in ('\x8942595A2dC5181Df0465AF0D7be08c8f23C93af', '\xb6029ea3b2c51d09a50b53ca8012feeb05bda35a', '\xae32496491b53841efb51829d6f886387708f99b', '\x34cfac646f301356faa8b21e94227e3583fe3f5f') -- mastercopy address v0.1.0, v1.0.0, v1.1.0, v1.1.1
    )
    select creation_block_number, creation_time, address, safe_type
    from safes;

INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', 'REFRESH MATERIALIZED VIEW gnosis_safe.view_safes')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
