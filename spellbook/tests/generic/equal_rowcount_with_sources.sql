{% test equal_rowcount_with_sources(model, evt_sources=[]) %}

    WITH
    model_count as (
    select count(*) as count_a from {{ model }}
    )
    ,sources_count as (
    select sum(count_b) as count_b
    from (
        {% for source in evt_sources %}
        select count(*) as count_b
        from {{ source }}
        where evt_block_time <= (select max(block_time) from {{ model }})
        {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
        ) b
    )

    ,unit_test as (
    select count_a, count_b, abs(count_a - count_b) as diff_count
    from model_count
    full outer join sources_count
    on 1=1
    )

    select * from unit_test where diff_count > 0
{% endtest %}
