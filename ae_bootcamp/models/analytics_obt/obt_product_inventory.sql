select 
    i.inventory_id,
    i.transaction_type,
    i.transaction_created_date,
    i.transaction_modified_date,
    i.product_id,
    i.quantity,
    i.purchase_order_id,
    i.customer_order_id,
    i.comments,
    -- p.product_id,
    p.product_code,
    p.product_name,
    p.description,
    p.supplier_company,
    p.standard_cost,
    p.list_price,
    p.reorder_level,
    p.target_level,
    p.quantity_per_unit,
    p.discontinued,
    p.minimum_reorder_quantity,
    p.category,
    p.attachments,
    -- d.date_id,
    -- d.full_date,
    -- d.year,
    -- d.year_week,
    -- d.year_day,
    -- d.fiscal_year,
    -- d.fiscal_quarter,
    -- d.month,
    -- d.month_name,
    -- d.week_day,
    -- d.day_name,
    -- d.day_is_weekend,
    current_timestamp() as ingestion_timestamp
from {{ ref('fact_inventory') }} i 
left join {{ ref('dim_product') }} p 
    on p.product_id = i.product_id
-- left join {{ ref('dim_date') }} d 
--     on d.full_date = i.transaction_created_date