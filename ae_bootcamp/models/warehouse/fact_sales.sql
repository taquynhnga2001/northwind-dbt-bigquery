{{
    config(partition_by={
        "field": "order_date",
        "data_type": "date"
    })
}}

with source as (
    select 
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        date(o.order_date) as order_date,
        o.shipped_date,
        o.paid_date,
        current_timestamp() as ingestion_timestamp
    from {{ ref('stg_order_details') }} as od 
    left join {{ ref('stg_orders') }} as o 
        on od.order_id = o.id
    where od.order_id is not null
),

unique_source as (
    select
        *,
        row_number() over(partition by order_id, product_id, customer_id, employee_id, shipper_id, status_id, purchase_order_id, inventory_id, order_date) as row_num
    from source
)

select *
except (row_num)
from unique_source
where row_num = 1