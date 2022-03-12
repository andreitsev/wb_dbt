{{
    config(
        materialized='table'
    )
}}

select
    barcode
    , supplierArticle
    , techSize
    , subject
    , category
    , brand
    , cast(date as date) as day

    , sum(case when quantity > 0 then quantity else 0 end) as total_q_purchases
    , case
        when sum(case when quantity > 0 then 1 else 0 end) > 0 then 
            sum(case when quantity > 0 then totalprice else 0 end) / sum(case when quantity > 0 then 1 else 0 end) 
        else null end as avg_totalprice_purchased
from
    {{ source('raw', 'raw_purchases') }}
group by
    barcode
    , supplierArticle
    , techSize
    , subject
    , category
    , brand
    , day 



