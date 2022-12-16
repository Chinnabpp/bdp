INSERT INTO cash_flow
    SELECT
        port_id,
        time_period_code,
        effective_date,
        net_cash_flow,
        subscriptions,
		redemptions,
        exchanges_in,
        exchanges_out,
        conversions_in,
        conversions_out,
        reinvested_income,
        reinvested_short_term_capital_gains,
        reinvested_long_term_capital_gains
    FROM 
        cash_flow_staging
    WHERE 
        file_key = '{0}'
ON CONFLICT (port_id, time_period_code, effective_date) DO UPDATE
SET
    port_id=excluded.port_id,
    time_period_code=excluded.time_period_code,
    effective_date=excluded.effective_date,
    net_cash_flow=excluded.net_cash_flow,
    subscriptions=excluded.subscriptions,
    redemptions=excluded.redemptions,
    exchanges_in=excluded.exchanges_in,
    exchanges_out=excluded.exchanges_out,
    conversions_in=excluded.conversions_in,
    conversions_out=excluded.conversions_out,
    reinvested_income=excluded.reinvested_income,
    reinvested_short_term_capital_gains=excluded.reinvested_short_term_capital_gains,
    reinvested_long_term_capital_gains=excluded.reinvested_long_term_capital_gains
