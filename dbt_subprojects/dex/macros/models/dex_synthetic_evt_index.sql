-- Trades decoded from call traces have no log index to carry. Numbering them 1..n per
-- transaction puts them in the same number space as the real log indices event-based
-- projects carry, so the two collide: on 2026-09-09 an origin_arm swap numbered 2
-- collided with a uniswap v4 swap whose real log index was 2, and the duplicate key
-- stalled the prices pipeline for 15 hours.
--
-- Each trace-derived project gets its own 1M-wide band, far above any log index a
-- transaction can carry (the busiest of the last 90 days reached 25,509). A shared
-- band would not do: it shifts every project equally, so pairs that already collide
-- still would -- 1inch-LOP and origin_arm share 137 keys over 30 days, since 1inch
-- limit orders route through Origin ARMs and both number from 1.
--
-- Prefer a real log index when the venue emits an event: see curve and ekubo, which
-- carry `index` from the chain's logs. Reach for a band only when there is nothing to
-- anchor to. Add a project here rather than numbering it from 1 -- an unregistered
-- name fails at compile time. Changing a project's numbering needs a full refresh of
-- its model: the incremental merge key is (tx_hash, evt_index), so a plain re-run
-- inserts renumbered rows alongside the old ones instead of replacing them.

{% macro dex_synthetic_evt_index_offset(project) -%}
    {%- set bands = {
        'origin_arm': 101000000,
        'tempo_exchange': 102000000,
        'oneinch_lop': 103000000,
        'zigzag': 104000000,
        'angstrom': 105000000
    } -%}
    {#- an unregistered name renders empty, leaving a unary plus and silently numbering from 1 again -#}
    {%- if project not in bands -%}
        {{ exceptions.raise_compiler_error("dex_synthetic_evt_index_offset: register a band for '" ~ project ~ "' in dex_synthetic_evt_index.sql") }}
    {%- endif -%}
    {{ bands[project] }}
{%- endmacro %}
