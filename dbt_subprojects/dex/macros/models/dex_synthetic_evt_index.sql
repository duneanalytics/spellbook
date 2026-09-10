-- Trades decoded from call traces have no log index to carry. Numbering them 1..n per
-- transaction puts them in the same number space as the real log indices event-based
-- projects carry, and (blockchain, tx_hash, evt_index) then stops identifying a single
-- trade: on 2026-09-09 an origin_arm swap numbered 2 collided with a uniswap v4 swap
-- whose real log index was 2, and the duplicate key stalled the prices pipeline for 15
-- hours. tempo_exchange numbers 99% of its rows 1 and collides the same way.
--
-- So offset synthetic numbering above any log index a transaction can carry -- the
-- busiest transaction of the last 30 days reached 20,032 -- and give every
-- trace-derived project its own band, so a synthetic index can collide neither with a
-- real log index nor with another project's synthetic one.
--
-- Prefer a real log index when the venue emits an event: see curve and ekubo, which
-- carry `index` from the chain's logs. Reach for a band only when there is no event to
-- anchor to. Register a new project below rather than numbering it from 1, and note
-- that changing a project's numbering needs a full refresh of its model -- the
-- incremental merge key is (tx_hash, evt_index), so a re-run would otherwise insert
-- renumbered rows alongside the old ones instead of replacing them.

{% macro dex_synthetic_evt_index_offset(project) -%}
    {%- set bands = {
        'origin_arm': 1000000,
        'tempo_exchange': 2000000,
        'oneinch_lop': 3000000,
        'zigzag': 4000000,
        'angstrom': 5000000
    } -%}
    {%- if project not in bands -%}
        {{ exceptions.raise_compiler_error(
            "dex_synthetic_evt_index_offset: no band registered for project '" ~ project ~ "'. "
            ~ "Add one to dex_synthetic_evt_index.sql; reusing another project's band reintroduces the collision."
        ) }}
    {%- endif -%}
    {{ bands[project] }}
{%- endmacro %}
