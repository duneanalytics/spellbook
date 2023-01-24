WITH unit_tests as
(
    SELECT
        bytea2numeric_v3(lpad('',64,'F')) as result
        ,'115792089237316195423570985008687907853269984665640564039457584007913129639935' as expected
)
select * from unit_tests
where result != expected
