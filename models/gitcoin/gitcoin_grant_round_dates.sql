{{ config(
        alias = alias('grant_round_dates', legacy_model=True)
        , tags=['legacy', 'static']
        )
}}

SELECT round_name, start_date, end_date
FROM (VALUES
    ('GR1', date('2019-02-01'), date('2019-02-15'))
    , ('GR2', date('2019-03-05'), date('2019-04-19'))
    , ('GR3', date('2019-09-15'), date('2019-10-02'))
    , ('GR4', date('2020-01-06'), date('2020-01-21'))
    , ('GR5', date('2020-03-23'), date('2020-04-04'))
    , ('GR6', date('2020-06-15'), date('2020-07-03'))
    , ('GR7', date('2020-09-15'), date('2020-10-02'))
    , ('GR8', date('2020-12-02'), date('2020-12-17'))
    , ('GR9', date('2021-03-10'), date('2021-03-25'))
    , ('GR10', date('2021-06-17'), date('2021-07-01'))
    , ('GR11', date('2021-09-08'), date('2021-09-23'))
    , ('GR12', date('2021-12-01'), date('2021-12-16'))
    , ('GR13', date('2022-03-09'), date('2022-03-24'))
    , ('GR14', date('2022-06-08'), date('2022-06-23'))
    , ('GR15', date('2022-09-07'), date('2022-09-22'))
    , ('Unicef Round', date('2022-12-09'), date('2022-12-21'))
    , ('Gitcoin Alpha Round', date('2023-01-17'), date('2023-01-31'))
    , ('Gitcoin Beta Round', date('2023-04-25'), date('2023-05-09'))
    ) AS temp_table (round_name, start_date, end_date)