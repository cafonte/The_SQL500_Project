create view "vw.altz_score"(symbol, shortname, alt_z, last_updated) as
WITH altz_test AS (SELECT bal.symbol,
                          cur.last_updated,
                          cur.shortname,
                          bal.working_capital / NULLIF(bal.total_assets, 0::double precision)                    AS a,
                          bal.retained_earnings / NULLIF(bal.total_assets, 0::double precision)                  AS b,
                          inc.ebit / NULLIF(bal.total_assets, 0::double precision)                               AS c,
                          cur.marketcap::double precision / NULLIF(bal.current_liabilities, 0::double precision) AS d,
                          inc.total_revenue / NULLIF(bal.total_assets, 0::double precision)                      AS e
                   FROM balance_sheet bal
                            JOIN income_sheet inc ON bal.symbol = inc.symbol
                            JOIN equity_info cur ON bal.symbol = cur.symbol)
SELECT symbol,
       shortname,
       CASE
           WHEN a = NULL::double precision THEN NULL::double precision
           WHEN b = NULL::double precision THEN NULL::double precision
           WHEN c = NULL::double precision THEN NULL::double precision
           WHEN d = NULL::double precision THEN NULL::double precision
           WHEN e = NULL::double precision THEN NULL::double precision
           ELSE 1.2::double precision * a + 1.4::double precision * b + 3.3::double precision * c +
                0.6::double precision * d + 1.0::double precision * e
           END AS alt_z,
       last_updated
FROM altz_test
