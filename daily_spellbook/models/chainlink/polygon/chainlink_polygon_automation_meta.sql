{{
  config(
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set chainlayer = 'chainlayer' %}
{% set cryptomanufaktur = 'cryptomanufaktur' %}
{% set dextrac = 'dextrac' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set linkpool = 'linkpool' %}
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
  ('{{chainlayer}}', 0xD2cEBc57ADc65d03Af829ccB53517a9b90e3219C),
  ('{{cryptomanufaktur}}', 0xd9a31dc8347284a6BEa9598d49d8DAcbFD5774F1),
  ('{{cryptomanufaktur}}', 0x27c143A6B7CfADB4686c1d06e45EaB79B53f82B6),
  ('{{dextrac}}', 0x9cB11Bb6568082502e46A43cE4fC48d6e07c567A),
  ('{{fiews}}', 0x4ED89b6E3eBE0FB4a97cb9Fa5C4FAB64b1045B39),
  ('{{fiews}}', 0x0c904E705943c4C1D8e56ba5062Fa005343F4bfd),
  ('{{fiews}}', 0xdDb52466E31F80E3269885BD161C08087b54B287),
  ('{{inotel}}', 0x71DD101432FD1041DfD7b718b1c0adeb1cE13d60),
  ('{{inotel}}', 0x858A480e5b92A8d9D8864785cbF11B4C5dD56Dfe),
  ('{{inotel}}', 0x99Fd8F54f0FD6AE073De12Ae7efe554B9A8b76C0),
  ('{{linkforest}}', 0x18542E08F4267C8ddCFEF6E6E692CeF8eF3A8365),
  ('{{linkforest}}', 0x5175d113eaC843Da5bff4a577C64789328AEbb4f),
  ('{{linkforest}}', 0x80aC52aC017f8cEbE579F40f2fFC5C3A7968De10),
  ('{{linkpool}}', 0xCe08a02e0f6858DDc0e31110972e8139Cf0a146d),
  ('{{linkriver}}', 0x8ABe586e96745D4acA358bAe11e71F5A59434f47),
  ('{{linkriver}}', 0xe58B9a01f01f444cd45fF47C41529d400adE9A52),
  ('{{linkriver}}', 0xA53e021Be83d0251eD58452Dd3FC1623186a6ad0),
  ('{{piertwo}}', 0x7a80bEaCcA09e2F3d8DDb7ABFdc975e1efB194d7),
  ('{{piertwo}}', 0x93fa1481BF95C47a9c6F26B8e70a81C7A607934b),
  ('{{piertwo}}', 0x3036fc59b1c457ab5336059d828518e6fDb54cfD),
  ('{{simplyvc}}', 0x953731C84798d6F64c795da8973f565761A8964C),
  ('{{simplyvc}}', 0x6c995b2abCbd72a4A35D80F9c5476f496Ee97bD1),
  ('{{simplyvc}}', 0x09DafcF34369842EB7E7a5662Bb6793171127Ed1),
  ('{{stakingfacilities}}', 0xd1Be7FcE9C87F22E3715d257FCE92F7595018B67),
  ('{{stakingfacilities}}', 0x19c67363742a485a7BE3DF520a889E5E3a73337A)
) a (operator_name, keeper_address)
