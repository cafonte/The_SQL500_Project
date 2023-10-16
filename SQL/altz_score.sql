-- Author: Chase Fonte
-- Last Updated: 10/16/2023

with altz_gather as (
        select
            bal.symbol,
            cur.last_updated,
            cur.shortname,
            bal.working_capital/NULLIF(bal.total_assets,0) as a,
            bal.retained_earnings/nullif(bal.total_assets,0) as b,
            inc.ebit/nullif(bal.total_assets,0) as c,
            cur.marketcap/nullif(bal.current_liabilities,0) as d,
            inc.total_revenue/nullif(bal.total_assets,0) as e
    from (select max(last_updated), symbol, working_capital, total_assets, retained_earnings, current_liabilities from balance_sheet
            group by symbol, working_capital, total_assets, retained_earnings, current_liabilities) as bal
        inner join (select max(last_updated), symbol, ebit, total_revenue from income_sheet
                        group by symbol, ebit, total_revenue) as inc on bal.symbol = inc.symbol
        inner join (select max(last_updated) as last_updated, symbol, shortname, marketcap from equity_info
                        group by symbol, shortname, marketcap) as cur on bal.symbol = cur.symbol
    where cur.last_updated = (select max(last_updated) from equity_info)
    ),
altz_build as (
        Select
            symbol,
            shortname,
            case when a = null then null
                when b = null then null
                when c = null then null
                when d = null then null
                when e = null then null
            else (1.2*a)+(1.4*b)+(3.3*c)+(0.6*d)+(1.0*e) end alt_z,
            last_updated
        from altz_gather
    )

select
    symbol,
    shortname,
    alt_z,
    case when alt_z <= 1.8 then 'In Distress'
        when alt_z between 1.8 and 3.0 then 'Gray Zone'
        when alt_z > 3.0 then 'Safe Zone'
        else null
    end altz_rank,
    last_updated

from altz_build
where alt_z > 0 --remove filter to show null companies
order by alt_z desc
