{#
  One-off repair for the 2026-02-04 1inch v1.2 LO -> CC reclassification.
  Keep the date literal: it is the frozen first affected day, not a rolling window.
  The source relations must be rebuilt before this operation runs.
#}
{% macro oneinch_reconcile_lop_trades(target_kind, blockchain, block_month, apply=false,
    expected_old_count=none, expected_desired_count=none, expected_delete_count=none,
    expected_insert_count=none, expected_update_count=none, allow_empty_source=false,
    restore_version=none, through_month=none) %}
    {% set affected_chains = [
        'arbitrum', 'avalanche_c', 'base', 'bnb', 'ethereum', 'gnosis',
        'linea', 'optimism', 'polygon', 'sonic', 'unichain', 'zksync'
    ] %}
    {% if blockchain not in affected_chains %}
        {{ exceptions.raise_compiler_error('Unsupported 1inch repair blockchain: ' ~ blockchain) }}
    {% endif %}
    {% if block_month | length != 10 or block_month[4] != '-' or block_month[7] != '-'
        or block_month[8:] != '01' or not (block_month[:4] ~ block_month[5:7] ~ block_month[8:]).isdigit()
        or block_month < '2026-02-01' %}
        {{ exceptions.raise_compiler_error('block_month must be YYYY-MM-01 on or after 2026-02-01') }}
    {% endif %}
    {% set through_month = block_month if through_month is none else through_month %}
    {% if through_month | length != 10 or through_month[4] != '-' or through_month[7] != '-'
        or through_month[8:] != '01' or not (through_month[:4] ~ through_month[5:7] ~ through_month[8:]).isdigit()
        or through_month < block_month %}
        {{ exceptions.raise_compiler_error('through_month must be YYYY-MM-01 on or after block_month') }}
    {% endif %}
    {% if target_kind == 'chain' %}
        {% set target_relation = ref('dex_' ~ blockchain ~ '_trades') %}
        {% set source_relation = ref('oneinch_' ~ blockchain ~ '_lop_own_trades') %}
        {% set keys = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month'] %}
    {% elif target_kind == 'dex' %}
        {% set target_relation = ref('dex_trades') %}
        {% set source_relation = ref('dex_' ~ blockchain ~ '_trades') %}
        {% set keys = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month'] %}
    {% elif target_kind == 'aggregator' %}
        {% set target_relation = ref('dex_aggregator_trades') %}
        {% set source_relation = ref('oneinch_lop_aggregator_trades') %}
        {% set keys = ['block_month', 'block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'] %}
    {% else %}
        {{ exceptions.raise_compiler_error('target_kind must be chain, dex, or aggregator') }}
    {% endif %}
    {% if restore_version is not none %}
        {% if not (restore_version | string).isdigit() %}
            {{ exceptions.raise_compiler_error('restore_version must be a nonnegative Delta version') }}
        {% endif %}
        {% set source_relation = target_relation %}
        {% set source_sql = source_relation ~ ' FOR VERSION AS OF ' ~ restore_version %}
    {% elif target_kind == 'chain' %}
        {% set source_sql = '(' ~ oneinch_lop_dex_trades_passthrough(blockchain=blockchain, full_history=true) ~ ')' %}
    {% else %}
        {% set source_sql = source_relation %}
    {% endif %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}
    {% if target.name != 'prod' %}
        {{ exceptions.raise_compiler_error('1inch repair operates only on the prod target') }}
    {% endif %}

    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set source_columns = adapter.get_columns_in_relation(source_relation) %}
    {% set source_names = [] %}
    {% for col in source_columns %}
        {% do source_names.append(col.name | lower) %}
    {% endfor %}
    {% set names = [] %}
    {% set content_names = [] %}
    {% for col in target_columns %}
        {% set name = col.name | lower %}
        {% do names.append(name) %}
        {% if name not in keys and name != '_updated_at' %}
            {% do content_names.append(name) %}
        {% endif %}
        {% if name not in source_names and not (target_kind in ['chain', 'aggregator'] and name == '_updated_at') %}
            {{ exceptions.raise_compiler_error('Repair source is missing target column: ' ~ name) }}
        {% endif %}
    {% endfor %}
    {% for key in keys %}
        {% if key not in names %}
            {{ exceptions.raise_compiler_error('Repair target is missing key column: ' ~ key) }}
        {% endif %}
    {% endfor %}

    {% set desired_columns = [] %}
    {% set insert_columns = [] %}
    {% set insert_values = [] %}
    {% set update_columns = [] %}
    {% for col in target_columns %}
        {% set name = col.name | lower %}
        {% do insert_columns.append('"' ~ name ~ '"') %}
        {% do insert_values.append('s."' ~ name ~ '"') %}
        {% if target_kind in ['chain', 'aggregator'] and restore_version is none and name == '_updated_at' %}
            {% do desired_columns.append('cast(current_timestamp as ' ~ col.data_type ~ ') as "' ~ name ~ '"') %}
        {% else %}
            {% do desired_columns.append('cast(src."' ~ name ~ '" as ' ~ col.data_type ~ ') as "' ~ name ~ '"') %}
        {% endif %}
        {% if name not in keys %}
            {% do update_columns.append('"' ~ name ~ '" = s."' ~ name ~ '"') %}
        {% endif %}
    {% endfor %}
    {% set join_conditions = [] %}
    {% set target_conditions = [] %}
    {% set desired_conditions = [] %}
    {% set old_nulls = [] %}
    {% set desired_nulls = [] %}
    {% set changes = [] %}
    {% set merge_changes = [] %}
    {% for key in keys %}
        {% do join_conditions.append('t."' ~ key ~ '" = d."' ~ key ~ '"') %}
        {% do target_conditions.append('t."' ~ key ~ '" = s."' ~ key ~ '"') %}
        {% do old_nulls.append('"' ~ key ~ '" is null') %}
        {% do desired_nulls.append('"' ~ key ~ '" is null') %}
    {% endfor %}
    {% for name in content_names %}
        {% do changes.append('t."' ~ name ~ '" is distinct from d."' ~ name ~ '"') %}
        {% do merge_changes.append('t."' ~ name ~ '" is distinct from s."' ~ name ~ '"') %}
    {% endfor %}
    {% set month_scope = "block_month = date '" ~ block_month ~ "'" if through_month == block_month
        else "block_month between date '" ~ block_month ~ "' and date '" ~ through_month ~ "'" %}
    {% set scope = "blockchain = '" ~ blockchain ~ "' and project = '1inch-LOP' and " ~ month_scope ~ " and block_date >= date '2026-02-04'" %}
    {% set target_scope = "t.blockchain = '" ~ blockchain ~ "' and t.project = '1inch-LOP' and t." ~ month_scope ~ " and t.block_date >= date '2026-02-04'" %}
    {% set desired_sql %}
        select {{ desired_columns | join(', ') }}
        from {{ source_sql }} as src
        where {{ scope }}
    {% endset %}
    {% set diff_sql %}
        with desired as ({{ desired_sql }}),
        old as (select * from {{ target_relation }} where {{ scope }}),
        paired as (
            select t.blockchain as old_marker, d.blockchain as desired_marker,
                {% if changes %}({{ changes | join(' or ') }}){% else %}false{% endif %} as changed
            from old as t full outer join desired as d on {{ join_conditions | join(' and ') }}
        )
        select
            (select count(*) from old) as old_count,
            (select count(*) from desired) as desired_count,
            count_if(desired_marker is null) as delete_count,
            count_if(old_marker is null) as insert_count,
            count_if(old_marker is not null and desired_marker is not null and changed) as update_count,
            (select count(*) from old where {{ old_nulls | join(' or ') }}) as old_null_keys,
            (select count(*) from desired where {{ desired_nulls | join(' or ') }}) as desired_null_keys,
            (select count(*) from (select {{ keys | join(', ') }} from old group by {{ keys | join(', ') }} having count(*) > 1)) as old_duplicate_keys,
            (select count(*) from (select {{ keys | join(', ') }} from desired group by {{ keys | join(', ') }} having count(*) > 1)) as desired_duplicate_keys
        from paired
    {% endset %}
    {% set diff = run_query(diff_sql) %}
    {% if diff is none or diff.rows | length != 1 %}
        {{ exceptions.raise_compiler_error('1inch repair diff returned no result') }}
    {% endif %}
    {% set row = diff.rows[0] %}
    {% set old_count = row[0] | int %}
    {% set desired_count = row[1] | int %}
    {% set delete_count = row[2] | int %}
    {% set insert_count = row[3] | int %}
    {% set update_count = row[4] | int %}
    {% set old_null_keys = row[5] | int %}
    {% set desired_null_keys = row[6] | int %}
    {% set old_duplicate_keys = row[7] | int %}
    {% set desired_duplicate_keys = row[8] | int %}
    {{ log('1inch repair ' ~ target_kind ~ '/' ~ blockchain ~ '/' ~ block_month ~ '..' ~ through_month ~
        ' restore_version=' ~ (restore_version if restore_version is not none else 'none') ~
        ' old=' ~ old_count ~ ' desired=' ~ desired_count ~ ' delete=' ~ delete_count ~
        ' insert=' ~ insert_count ~ ' update=' ~ update_count ~
        ' null_keys=' ~ (old_null_keys + desired_null_keys) ~
        ' duplicate_keys=' ~ (old_duplicate_keys + desired_duplicate_keys), info=true) }}
    {% if old_null_keys + desired_null_keys + old_duplicate_keys + desired_duplicate_keys != 0 %}
        {{ exceptions.raise_compiler_error('1inch repair key preflight failed') }}
    {% endif %}
    {% set merge_sql %}
        merge into {{ target_relation }} as t
        using (
            with desired as ({{ desired_sql }}),
            old_only as (
                select t.*
                from {{ target_relation }} as t
                left join desired as d on {{ join_conditions | join(' and ') }}
                where {{ target_scope }} and d.blockchain is null
            )
            select d.*, false as _delete from desired as d
            union all
            select o.*, true as _delete from old_only as o
        ) as s
        on {{ target_conditions | join(' and ') }}
            and t.blockchain = '{{ blockchain }}'
            and t.project = '1inch-LOP'
            and t.{{ month_scope }}
            and t.block_date >= date '2026-02-04'
        when matched and s._delete then delete
        when matched and ({{ merge_changes | join(' or ') }}) then
            update set {{ update_columns | join(', ') }}
        when not matched and not s._delete then
            insert ({{ insert_columns | join(', ') }}) values ({{ insert_values | join(', ') }})
    {% endset %}
    {# EXPLAIN plans but does not execute the MERGE. It tests the exact SQL and service
       identity's DELETE/UPDATE permission, including the self-read in old_only. #}
    {% if not apply %}
        {% do run_query('EXPLAIN ' ~ merge_sql) %}
        {{ log('1inch repair MERGE EXPLAIN passed for ' ~ target_kind ~ '/' ~ blockchain ~ '/' ~ block_month ~ '..' ~ through_month, info=true) }}
        {{ return({'old': old_count, 'desired': desired_count, 'delete': delete_count,
            'insert': insert_count, 'update': update_count}) }}
    {% endif %}
    {% if desired_count == 0 and old_count > 0 and not allow_empty_source %}
        {{ exceptions.raise_compiler_error('Empty desired source requires allow_empty_source=true') }}
    {% endif %}
    {% if expected_old_count is none or expected_desired_count is none or expected_delete_count is none
        or expected_insert_count is none or expected_update_count is none %}
        {{ exceptions.raise_compiler_error('Apply requires all five expected counts from a read-only preflight') }}
    {% endif %}
    {% if old_count != expected_old_count | int or desired_count != expected_desired_count | int
        or delete_count != expected_delete_count | int or insert_count != expected_insert_count | int
        or update_count != expected_update_count | int %}
        {{ exceptions.raise_compiler_error('1inch repair source/target changed since preflight') }}
    {% endif %}
    {% if delete_count + insert_count + update_count == 0 %}
        {{ log('1inch repair already matches desired source; no write needed', info=true) }}
        {{ return({'old': old_count, 'desired': desired_count, 'delete': 0, 'insert': 0, 'update': 0}) }}
    {% endif %}

    {% do run_query(merge_sql) %}
    {% set after = run_query(diff_sql) %}
    {% if after.rows[0][2] | int != 0 or after.rows[0][3] | int != 0 or after.rows[0][4] | int != 0
        or after.rows[0][0] | int != after.rows[0][1] | int %}
        {{ exceptions.raise_compiler_error('1inch repair committed but post-merge parity failed; inspect Delta history before retry') }}
    {% endif %}
    {{ log('1inch repair verified ' ~ target_kind ~ '/' ~ blockchain ~ '/' ~ block_month, info=true) }}
    {{ return({'old': old_count, 'desired': desired_count, 'delete': delete_count,
        'insert': insert_count, 'update': update_count}) }}
{% endmacro %}
