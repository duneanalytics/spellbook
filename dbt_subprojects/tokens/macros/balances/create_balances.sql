        WHEN b.token_standard = 'native' THEN b.balance_raw / power(10, 18)
