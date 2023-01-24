{% test zeroex_ethereum_nft_test(model, column_name, seed_file) %}


WITH unit_tests AS
(
    SELECT 
        CASE WHEN 
            fills.{{ column_name }} = fills_sample.{{ column_name }}
            THEN True ELSE False END 
        AS amount_test
    FROM {{ model }} fills
    JOIN {{ seed_file }} fills_sample 
        ON fills.tx_hash = fills_sample.tx_hash
        AND fills.evt_index = fills_sample.evt_index
)
select *
    from unit_tests
    where amount_test = True

{% endtest %}
