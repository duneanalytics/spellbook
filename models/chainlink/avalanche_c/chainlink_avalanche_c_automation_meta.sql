{{
  config(
    tags=['dunesql'],
    alias=alias('automation_meta'),
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set alphachain = 'alphachain' %}
{% set blockdaemon = 'blockdaemon' %}
{% set dextrac = 'dextrac' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set lexisnexis = 'lexisnexis' %}
{% set linkforest = 'linkforest' %}
{% set linkpool = 'linkpool' %}
{% set linkriver = 'linkriver' %}
{% set simplyvc = 'simplyvc' %}
{% set validationcapital = 'validationcapital' %}

SELECT
   'avalanche_c' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0xe0b4fb0822d1f71977B33109F9F84948d4c31e4A),
  ('{{alphachain}}', 0xcB3Ef22D906a3518EF3FB318DFaf94C039ee683c),
  ('{{blockdaemon}}', 0xA18cE786F361a00CB830E87F3B1179c5ac89484E),
  ('{{dextrac}}', 0xCf3C657abF393D4425d938d7DC35AF774fa31410),
  ('{{fiews}}', 0x23C3B573aec3D978082F8F4DfAe1B9b57c658Fb9),
  ('{{inotel}}', 0x3965BCE1B6F4B872d0C6e3A2EfC5ecF39ec2c883),
  ('{{lexisnexis}}', 0xA7542d863e6bcCF9DFa4db11fD5823dd56022f34),
  ('{{linkforest}}', 0x762A9C7c6EC4b80ef01ac89D72A0eB5731Dd3447),
  ('{{linkpool}}', 0xF12930fE0d73957Bac81C4A44000891A69219157),
  ('{{linkriver}}', 0x5F5fc989ea771E07dc01db04BeE543b9bab2D5E1),
  ('{{simplyvc}}', 0x648715137b75f40c9F8DC17701d0BEd43958771f),
  ('{{validationcapital}}', 0xDb1e4d2378B20E8bc933b134395279b0ddB8e682)
) a (operator_name, keeper_address)
