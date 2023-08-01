{{ config(
        alias = alias('erc20_all',legacy_model=True)
        ,tags=['legacy']
        )
}}


        SELECT '1' as blockchain, '0x' as contract_address, 'erc20' as standard

        from {{ source('erc20_' + chain , 'evt_transfer') }} tr 

        WHERE 1=1
        {% if is_incremental() %}
        and tr.evt_block_time >= date_trunc('day', now() - interval '7' day)
        AND contract_address NOT IN (
                                SELECT contract_address
                                from {{this}} t
                                WHERE t.blockchain = '{{chain}}'
                                )
        {% endif %}
        GROUP BY 1,2,3 --uniques
        
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}