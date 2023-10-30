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
            evt_index
        from {{ model }}
        where block_date <= (TIMESTAMP '{{ end_date }}' - interval '1' second)
        order by block_time desc
        limit {{ sample_size * 2 }}

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
            evt_index
        from {{ compare_model }}
        where block_date <= TIMESTAMP '{{ end_date }}'
        order by block_time desc
        limit {{ sample_size }}

    ),

    matched_records as (
        select
            seed.blockchain as seed_blockchain,
            model_sample.blockchain as model_blockchain,

            seed.project as seed_project,
            model_sample.project as model_project,

            seed.version as seed_version,
            model_sample.version as model_version,

            seed.tx_hash as seed_tx_hash,
            model_sample.tx_hash as model_tx_hash,

            seed.evt_index as seed_evt_index,
            model_sample.evt_index as model_evt_index,

            seed.token_bought_address as seed_token_bought_address,
            model_sample.token_bought_address as model_token_bought_address ,

            seed.token_sold_address as seed_token_sold_address,
            model_sample.token_sold_address as model_token_sold_address
            from b seed
        left join (
            select
                model.blockchain,

                model.project,

                model.version,

                model.tx_hash,

                model.evt_index,

                model.token_bought_address ,

                model.token_sold_address
                from b seed
            inner join a model
                ON 1=1

                    AND seed.blockchain = model.blockchain



                    AND seed.project = model.project



                    AND seed.version = model.version



                    AND seed.tx_hash = model.tx_hash



                    AND seed.evt_index = model.evt_index

                    ) model_sample
        ON 1=1

            AND seed.blockchain = model_sample.blockchain



            AND seed.project = model_sample.project



            AND seed.version = model_sample.version



            AND seed.tx_hash = model_sample.tx_hash



            AND seed.evt_index = model_sample.evt_index

            WHERE 1=1),

    -- check if the matching columns return singular results
    matching_count_test as (
        select
            'matched records count' as test_description,
            -- these are cast to varchar to unify column types, note this is only for displaying them in the test results
            cast(count(model_blockchain) as varchar) as result_model,
            cast(1 as varchar) as expected_seed,
            (count(model_blockchain) = 1) as equality_check,
            seed_blockchain as blockchain,

            seed_project as project,

            seed_version as version,

            seed_tx_hash as tx_hash,

            seed_evt_index as evt_index
            from matched_records
        GROUP BY
            seed_blockchain ,

            seed_project ,

            seed_version ,

            seed_tx_hash ,

            seed_evt_index
            ) ,

    equality_tests as
    (
        select
            'equality test: token_bought_address' as test_description,
            -- these are cast to varchar to unify column types, note this is only for displaying them in the test results
            cast(model_token_bought_address as varchar) as result_model,
            cast(seed_token_bought_address as varchar) as expected_seed,
            (model_token_bought_address IS NOT DISTINCT FROM seed_token_bought_address) as equality_check,
            seed_blockchain as blockchain,

            seed_project as project,

            seed_version as version,

            seed_tx_hash as tx_hash,

            seed_evt_index as evt_index
            from matched_records
        UNION ALL

        select
            'equality test: token_sold_address' as test_description,
            -- these are cast to varchar to unify column types, note this is only for displaying them in the test results
            cast(model_token_sold_address as varchar) as result_model,
            cast(seed_token_sold_address as varchar) as expected_seed,
            (model_token_sold_address IS NOT DISTINCT FROM seed_token_sold_address) as equality_check,
            seed_blockchain as blockchain,

            seed_project as project,

            seed_version as version,

            seed_tx_hash as tx_hash,

            seed_evt_index as evt_index
            from matched_records)


    select * from (
        select *
        from matching_count_test
        union all
        select *
        from equality_tests
    ) all
    -- equality check can be null so we have to check explicitly for nulls
    where equality_check is distinct from true

{% endtest %}