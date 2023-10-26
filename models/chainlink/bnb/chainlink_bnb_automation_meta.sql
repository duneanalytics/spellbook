{{
  config(
    
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set alphachain = 'alphachain' %}
{% set easy2stake = 'easy2stake' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set piertwo = 'piertwo' %}
{% set simplyvc = 'simplyvc' %}

SELECT
   'bnb' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0xd0C77d2E37d87E14922B07C48D448251ea3A6141),
  ('{{_01node}}', 0x43028Ea36A0db9BaBe5D0Bd9DA00d010aC135c37),
  ('{{alphachain}}', 0x7828bD0D31aaE91193676139d96d2572942cC489),
  ('{{alphachain}}', 0x22901bdd0ACC34F435F57CAD1F0A9C5957136F8C),
  ('{{easy2stake}}', 0x3c2a32898f6504BC35e8e3689d13A8E9A114B7b5),
  ('{{easy2stake}}', 0xA4e2864F6541721dFa36F96e8D868E0AdBD94881),
  ('{{fiews}}', 0x2c86d590177554E1A99343cb18f095D5e099c4D7),
  ('{{fiews}}', 0x765e469537F580f16B8C4bCF5BD4D823761678ad),
  ('{{inotel}}', 0x4922449C17Cee53b262191834FFf6fbc82A80f9C),
  ('{{inotel}}', 0xdd381028cfa5241284D23CE73ef7F0E3042d80F8),
  ('{{linkforest}}', 0x604410B182CDe4eCEB191Cfa2E0FD33224024c83),
  ('{{linkforest}}', 0x80C0EB96c401a441DF6D19ECd8e562b18C4E4E24),
  ('{{piertwo}}', 0xDed3787602432bc12271C467Bb02138b0Ee79923),
  ('{{piertwo}}', 0x60a764804dC2FaA78e06C1f09C1fc7236a3A7B9E),
  ('{{simplyvc}}', 0xBa61a9E217306315C239E73597d410e1bd469420),
  ('{{simplyvc}}', 0x07ACeD52eeBbd1642799fb48bBEaD5Bc64616341)
) a (operator_name, keeper_address)
