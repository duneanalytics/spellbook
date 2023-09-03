{% macro format_figure() %}
   CREATE OR REPLACE FUNCTION format_figure(figure, round_to, currency STRING)
   RETURNS STRING
   RETURN
   SELECT
      CASE
         WHEN CAST(figure as double) >= 1000000000000 THEN currency || format(concat('%1.', CAST(round_to as VARCHAR), 'f'), CAST(figure as double)/1e12) || 'T'
         WHEN CAST(figure as double) >= 1000000000 THEN currency || format(concat('%1.', CAST(round_to as VARCHAR), 'f'), CAST(figure as double)/1e9) || 'B'
         WHEN CAST(figure as double) >= 1000000 THEN currency || format(concat('%1.', CAST(round_to as VARCHAR), 'f'), CAST(figure as double)/1e6) || 'M'
         WHEN CAST(figure as double) >= 1000 THEN currency || format(concat('%1.', CAST(round_to as VARCHAR), 'f'), CAST(figure as double)/1e3) || 'K'
         ELSE currency || format(concat('%1.', CAST(round_to as VARCHAR), 'f'), CAST(figure as double))
      END as formatted_figures;
{% endmacro %}