{% test compare_dex_trades_sample(model, compare_model, end_date = '2023-01-01', sample_size = 100) %}

    with a as (

        select
            blockchain,
            project,
            version,
            block_date,
            block_time,
            token_bought_amount_raw,
            token_sold_amount_raw,
            token_bought_address,
            token_sold_address,
            taker,
            maker,
            project_contract_address,
            tx_hash,
            evt_index,
            tx_from,
            tx_to
        from {{ model }}
        where block_date <= TIMESTAMP '{{ end_date }}'
        order by block_time desc
        limit {{ sample_size }}

    ),
    b as (

        select
            blockchain,
            project,
            version,
            block_date,
            block_time,
            token_bought_amount_raw,
            token_sold_amount_raw,
            token_bought_address,
            token_sold_address,
            taker,
            maker,
            project_contract_address,
            tx_hash,
            evt_index,
            tx_from,
            tx_to
        from {{ compare_model }}
        where block_date <= TIMESTAMP '{{ end_date }}'
        order by block_time desc
        limit {{ sample_size }}

    )

    {%- set seed_check_columns = ['token_bought_address','token_sold_address'] -%}
    {%- set seed_matching_columns = ['blockchain','project','version','tx_hash','evt_index'] -%}

    {{ check_seed_macro(a, b, seed_matching_columns, seed_check_columns) }}

{% endtest %}