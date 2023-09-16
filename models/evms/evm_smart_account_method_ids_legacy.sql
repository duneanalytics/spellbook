
{{ config(
        schema = 'evms',
        tags = ['legacy','static'],
        alias = alias('evm_smart_account_method_ids', legacy_model=True)
        )
}}


SELECT 1