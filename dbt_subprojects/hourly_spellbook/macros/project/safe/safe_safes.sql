{% macro safe_safes_creation(blockchain, project_start_date=none, version_mapping=none, date_filter=false) %}
    {%- set network_config = get_safe_network_config(blockchain) -%}
    {%- set start_date = project_start_date if project_start_date else network_config.start_date -%}
    
    {#- Get version mappings from centralized source -#}
    {%- set deployments = get_official_safe_deployments() -%}
    {%- set default_version_mapping = {} -%}
    {%- for address, info in deployments.items() -%}
        {%- set _ = default_version_mapping.update({address: info.version}) -%}
    {%- endfor -%}
    
    {%- set final_version_mapping = default_version_mapping.copy() -%}
    {%- if version_mapping -%}
        {%- do final_version_mapping.update(version_mapping) -%}
    {%- endif -%}

select
    '{{ blockchain }}' as blockchain,
    et."from" as address,
    case 
        {% for address, version in final_version_mapping.items() %}
        {% set deployment_info = deployments.get(address, {}) %}
        when LOWER(CAST(et.to AS VARCHAR)) = LOWER('{{ address }}') then '{{ version }}'{{ '  -- ' ~ deployment_info.get('note', '') if deployment_info.get('note') else '' }}
        {% endfor %}
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    CAST(date_trunc('month', et.block_time) as DATE) as block_month,
    et.block_time as creation_time,
    et.tx_hash
from {{ source(blockchain, 'traces') }} et 
where et.success = true
    and LOWER(CAST(et.to AS VARCHAR)) in (
        {%- for address in final_version_mapping.keys() %}
        LOWER('{{ address }}'){{ ',' if not loop.last }}
        {%- endfor %}
    )
    and et.call_type = 'delegatecall' -- delegatecall to singleton is Safe (proxy) address
    and bytearray_substring(et.input, 1, 4) in (
        0x0ec78d9e, -- setup method v0.1.0
        0xa97ab18a, -- setup method v1.0.0
        0xb63e800d -- setup method v1.1.0, v1.1.1, v1.2.0, v1.3.0, v1.3.0L2, v1.4.1, v1.4.1L2, v1.5.0, v1.5.0L2
    )
    and et.gas_used > 10000  -- to ensure the setup call was successful. excludes e.g. setup calls with missing params that fallback
    {% if date_filter %}
    and et.block_time >= date_trunc('day', now() - interval '7' day)
    {% elif not is_incremental() %}
    and et.block_time > TIMESTAMP '{{ start_date }}' -- for initial query optimisation
    {% elif is_incremental() %}
    and et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
{% endmacro %}