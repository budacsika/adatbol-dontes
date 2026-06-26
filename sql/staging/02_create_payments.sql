create or replace table `@@PROJECT_ID@@.stg_arpadent.payments` as
select
    safe_cast(id as int64) as id,
    safe_cast(client_id as int64) as client_id,
    safe_cast(discount_amount as numeric) as discount_amount,
    safe_cast(billed_price_sum as numeric) as billed_price_sum,
    safe_cast(inserted_ts as datetime)        as inserted_ts,
    safe_cast(payment_storno_ts as datetime)  as payment_storno_ts
from `@@PROJECT_ID@@.raw_arpadent.payments_raw`
where billed_price_sum is not null;