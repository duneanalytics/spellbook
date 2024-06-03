{{
  config(
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set cryptomanufaktur = 'cryptomanufaktur' %}
{% set dextrac = 'dextrac' %}
{% set easy2stake = 'easy2stake' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set linkpool = 'linkpool' %}
{% set linkriver = 'linkriver' %}
{% set newroad = 'newroad' %}
{% set p2p = 'p2p' %}
{% set syncnode = 'syncnode' %}

SELECT
   'fantom' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0x1C5CAC6A8720999Fa7055fB7a756C9cEf45D75af),
  ('{{cryptomanufaktur}}', 0x37f52282fe101f250b0A984D1ab9FDD23c0aA55a),
  ('{{dextrac}}', 0xE79a35f22C384925b5ffac65CBB057a1c97D1991),
  ('{{easy2stake}}', 0x1FBabc5a710c22964c1277385537C509305D0da7),
  ('{{fiews}}', 0xa51eD04ea219C2E6F5Cdc67DE56B4D557108EdCB),
  ('{{inotel}}', 0x663aE03ac058c3772aEcB2D1fA82dCe52F3bB76A),
  ('{{linkforest}}', 0x56E01e5D6edF9d576916DE4D2f53c29d003F430e),
  ('{{linkpool}}', 0x5a13898549B17A78e63CA23aa34ccE57F58BbBc7),
  ('{{linkriver}}', 0x2284225C657771F4349243D40DcfFe79dE267CCE),
  ('{{newroad}}', 0x4B7376cc719f7d550f965acb0eaD2FA2BABAaF1a),
  ('{{p2p}}', 0xca95881059b985066cE93E257F3C74DCDBe1f1d4),
  ('{{syncnode}}', 0xedB6dEa5c88aBc0b57A1f94eDAfbA1f17DA1AeDE)
) a (operator_name, keeper_address)
