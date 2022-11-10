{% test zeroex_api_ethereum_fills_test(model, column_name, seed_file) %}


WITH unit_tests AS
-- (
    SELECT 
        -- CASE WHEN 
        --     fills.{{ column_name }} = fills_sample.{{ column_name }}
        --     THEN True ELSE False END 
        -- AS amount_test
        -- doing a hack here to figure the unique id test failure
        tx_hash, evt_index, count(*)

FROM {{ model }} fills
    left JOIN {{ seed_file }} fills_sample 
    ON fills.tx_hash = fills_sample.tx_hash
    AND fills.evt_index = fills_sample.evt_index
group by 1, 2 order by 3 desc limit 10
-- )
-- select *
--     from unit_tests
--     where amount_test = False

{% endtest %}