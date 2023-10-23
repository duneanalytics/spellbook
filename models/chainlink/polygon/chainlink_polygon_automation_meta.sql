{{
  config(
    
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set cryptomanufaktur = 'cryptomanufaktur' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set linkriver = 'linkriver' %}
{% set piertwo = 'piertwo' %}
{% set simplyvc = 'simplyvc' %}
{% set stakingfacilities = 'stakingfacilities' %}

SELECT
   'polygon' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0xA7d87D93Fc879c5ec959EbdC4a058bC906be42EC),
  ('{{_01node}}', 0x8B544FcE2177A84FFDb42800e947747A6277f5Ab),
  ('{{cryptomanufaktur}}', 0x27c143A6B7CfADB4686c1d06e45EaB79B53f82B6),
  ('{{cryptomanufaktur}}', 0xd9a31dc8347284a6BEa9598d49d8DAcbFD5774F1),
  ('{{fiews}}', 0x0c904E705943c4C1D8e56ba5062Fa005343F4bfd),
  ('{{fiews}}', 0x4ED89b6E3eBE0FB4a97cb9Fa5C4FAB64b1045B39),
  ('{{inotel}}', 0x858A480e5b92A8d9D8864785cbF11B4C5dD56Dfe),
  ('{{inotel}}', 0x71DD101432FD1041DfD7b718b1c0adeb1cE13d60),
  ('{{linkforest}}', 0x5175d113eaC843Da5bff4a577C64789328AEbb4f),
  ('{{linkforest}}', 0x18542E08F4267C8ddCFEF6E6E692CeF8eF3A8365),
  ('{{linkriver}}', 0xe58B9a01f01f444cd45fF47C41529d400adE9A52),
  ('{{linkriver}}', 0x8ABe586e96745D4acA358bAe11e71F5A59434f47),
  ('{{piertwo}}', 0x93fa1481BF95C47a9c6F26B8e70a81C7A607934b),
  ('{{piertwo}}', 0x7a80bEaCcA09e2F3d8DDb7ABFdc975e1efB194d7),
  ('{{simplyvc}}', 0x6c995b2abCbd72a4A35D80F9c5476f496Ee97bD1),
  ('{{simplyvc}}', 0x953731C84798d6F64c795da8973f565761A8964C),
  ('{{stakingfacilities}}', 0x19c67363742a485a7BE3DF520a889E5E3a73337A),
  ('{{stakingfacilities}}', 0xd1Be7FcE9C87F22E3715d257FCE92F7595018B67)
) a (operator_name, keeper_address)
