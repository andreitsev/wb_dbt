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

    , sum(case when quantity > 0 then quantity else 0 end) as total_q_sold
    , sum(case when quantity < 0 then abs(quantity) else 0 end) as total_q_returned
    , case
        when sum(case when quantity > 0 then 1 else 0 end) > 0 then 
            sum(case when quantity > 0 then totalprice else 0 end) / sum(case when quantity > 0 then 1 else 0 end) 
        else null end as avg_totalprice_sold
    , case
        when sum(case when quantity < 0 then 1 else 0 end) > 0 then 
            sum(case when quantity < 0 then abs(totalprice) else 0 end) / sum(case when quantity < 0 then 1 else 0 end) 
        else null end as avg_totalprice_returned

from
    {{ source('raw', 'raw_sales') }}
group by
    barcode
    , supplierArticle
    , techSize
    , subject
    , category
    , brand
    , day 



