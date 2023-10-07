{% test equal_rowcount(model, compare_model) %}

    with a as (

        select
          1 as id_dbtutils_test_equal_rowcount,
          count(*) as count_a
        from {{ model }}


    ),
    b as (

        select
          1 as id_dbtutils_test_equal_rowcount,
          count(*) as count_b
        from {{ compare_model }}

    ),
    final as (

        select
            count_a,
            count_b,
            abs(count_a - count_b) as diff_count

        from a
        full join b
        on
        a.id_dbtutils_test_equal_rowcount = b.id_dbtutils_test_equal_rowcount
    )

    select * from final where diff_count > 0

{% endtest %}