{{
  config(
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set alphachain = 'alphachain' %}
{% set chainlayer = 'chainlayer' %}
{% set dextrac = 'dextrac' %}
{% set easy2stake = 'easy2stake' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set linkpool = 'linkpool' %}
{% set linkriver = 'linkriver' %}
{% set piertwo = 'piertwo' %}
{% set simplyvc = 'simplyvc' %}

SELECT
   'bnb' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0xd0C77d2E37d87E14922B07C48D448251ea3A6141),
  ('{{_01node}}', 0x43028Ea36A0db9BaBe5D0Bd9DA00d010aC135c37),
  ('{{alphachain}}', 0x22901bdd0ACC34F435F57CAD1F0A9C5957136F8C),
  ('{{alphachain}}', 0x7828bD0D31aaE91193676139d96d2572942cC489),
  ('{{chainlayer}}', 0x228aB6d3e8bEbd6FA30B50E478D87A510617AA0f),
  ('{{dextrac}}', 0x24753929D0848988f2CF3d779CFe3211CD43b379),
  ('{{easy2stake}}', 0xA4e2864F6541721dFa36F96e8D868E0AdBD94881),
  ('{{easy2stake}}', 0x3c2a32898f6504BC35e8e3689d13A8E9A114B7b5),
  ('{{fiews}}', 0x765e469537F580f16B8C4bCF5BD4D823761678ad),
  ('{{fiews}}', 0x2c86d590177554E1A99343cb18f095D5e099c4D7),
  ('{{fiews}}', 0xA13fDb8Dda2196091E993498C7F2A3183A3ED58a),
  ('{{inotel}}', 0xdd381028cfa5241284D23CE73ef7F0E3042d80F8),
  ('{{inotel}}', 0x7D23dd26E2F0465150625667d224BBD3B35E2b17),
  ('{{inotel}}', 0x4922449C17Cee53b262191834FFf6fbc82A80f9C),
  ('{{linkforest}}', 0x80C0EB96c401a441DF6D19ECd8e562b18C4E4E24),
  ('{{linkforest}}', 0x604410B182CDe4eCEB191Cfa2E0FD33224024c83),
  ('{{linkforest}}', 0xa7d2C71Eaa52DdD12eB4C48C52318eaa82C5bb37),
  ('{{linkpool}}', 0xB9a93AADAB82a903eC43967F4C9FEf9297116D90),
  ('{{linkriver}}', 0x017B3bc89c1EA9b7F02f1F01B8E667290e9c1ff4),
  ('{{piertwo}}', 0x60a764804dC2FaA78e06C1f09C1fc7236a3A7B9E),
  ('{{piertwo}}', 0xDed3787602432bc12271C467Bb02138b0Ee79923),
  ('{{piertwo}}', 0x8b4b6886dFAfD77Bd8d8ddF84Ad3a0F2d1Cad936),
  ('{{simplyvc}}', 0x07ACeD52eeBbd1642799fb48bBEaD5Bc64616341),
  ('{{simplyvc}}', 0xBa61a9E217306315C239E73597d410e1bd469420),
  ('{{simplyvc}}', 0x2CCADCa3Dc99a6d55E2588e4255215b90Cff3320)
) a (operator_name, keeper_address)
