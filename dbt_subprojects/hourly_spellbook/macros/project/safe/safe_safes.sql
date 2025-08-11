{% macro safe_safes_creation(blockchain, project_start_date=none, version_mapping=none) %}
    {%- set network_config = get_safe_network_config(blockchain) -%}
    {%- set start_date = project_start_date if project_start_date else network_config.start_date -%}
    {%- set default_version_mapping = {
        '0x8942595A2dC5181Df0465af0D7be08c8f23C93af': '0.1.0',
        '0xb6029EA3B2c51D09a50B53CA8012FEeB05bDa35A': '1.0.0',
        '0xae32496491b53841efb51829d6f886387708F99B': '1.1.0',
        '0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F': '1.1.1',
        '0x6851D6fDfAfD08c0295C392436245E5bc78B0185': '1.2.0',
        '0xd9Db270c1B5E3Bd161E8c8503c55cEAbeE709552': '1.3.0',
        '0x69f4D1788e39c87893C980c06EdF4b7f686e2938': '1.3.0',
        '0x3E5c63644E683549055b9Be8653de26E0B4CD36E': '1.3.0L2',
        '0xfb1bffC9d739B8D520DaF37dF666da4C687191ea': '1.3.0L2',
        '0x41675C099F32341bf84BFc5382aF534df5C7461a': '1.4.1',
        '0x29fcB43b46531BcA003ddC8FCB67FFE91900C762': '1.4.1L2'
    } -%}
    
    {%- if version_mapping -%}
        {%- set final_version_mapping = default_version_mapping.update(version_mapping) or default_version_mapping -%}
    {%- else -%}
        {%- set final_version_mapping = default_version_mapping -%}
    {%- endif -%}

select
    '{{ blockchain }}' as blockchain,
    et."from" as address,
    case 
        {%- for address, version in final_version_mapping.items() %}
        when et.to = {{ address }} then '{{ version }}'{{ '  -- for chains with EIP-155' if address in ['0x69f4D1788e39c87893C980c06EdF4b7f686e2938', '0xfb1bffC9d739B8D520DaF37dF666da4C687191ea'] else '' }}
        {%- endfor %}
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    CAST(date_trunc('month', et.block_time) as DATE) as block_month,
    et.block_time as creation_time,
    et.tx_hash
from {{ source(blockchain, 'traces') }} et 
join {{ ref('safe_' ~ blockchain ~ '_singletons') }} s
    on et.to = s.address
where et.success = true
    and et.call_type = 'delegatecall' -- delegatecall to singleton is Safe (proxy) address
    and bytearray_substring(et.input, 1, 4) in (
        0x0ec78d9e, -- setup method v0.1.0
        0xa97ab18a, -- setup method v1.0.0
        0xb63e800d -- setup method v1.1.0, v1.1.1, v1.2.0, v1.3.0, v1.3.0L2, v1.4.1, v.1.4.1L2
    )
    and et.gas_used > 10000  -- to ensure the setup call was successful. excludes e.g. setup calls with missing params that fallback
    {% if not is_incremental() %}
    and et.block_time > TIMESTAMP '{{ start_date }}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    and et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
{% endmacro %}
