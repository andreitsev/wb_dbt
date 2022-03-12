{{
    config(
        materialized='table'
    )
}}

with distinct_sales_keys as
(
    select
        distinct 
            barcode
            , supplierarticle 
            , techsize
    from
        {{ source('raw', 'raw_sales') }}
    where techsize not like ''
),

distinct_purchases_keys as
(
    select
        distinct 
            barcode
            , supplierarticle 
            , techsize
    from
        {{ source('raw', 'raw_purchases') }}
    where techsize not like ''
),

distinct_storage_keys as
(
    select
        distinct 
            barcode
            , supplierarticle 
            , techsize
    from
        {{ source('raw', 'raw_storage') }}
    where techsize not like ''
),

distinct_supplies_keys as
(
    select
        distinct 
            barcode
            , supplierarticle 
            , techsize
    from
        {{ source('raw', 'raw_supplies') }}
    where techsize not like ''
)

select 
    distinct t.*
from
    (
        select * from distinct_sales_keys
        union
        select * from distinct_purchases_keys
        union 
        select * from distinct_storage_keys
        union
        select * from distinct_supplies_keys
    ) t






