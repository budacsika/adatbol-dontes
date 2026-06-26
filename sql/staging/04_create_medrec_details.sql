create or replace table `@@PROJECT_ID@@.stg_arpadent.medrec_details` as
select
    safe_cast(id as int64) as id,
    safe_cast(paym_id as int64) as paym_id, 
    safe_cast(client_id as int64) as client_id, 
    safe_cast(doctor_user_id as int64) as doctor_user_id, 
    safe_cast(medrec_plan_id as int64) as medrec_plan_id, 
    safe_cast(medrec_plan_detail_id as int64) as medrec_plan_detail_id, 
    safe_cast(treatment_medrec_id as int64) as treatment_medrec_id, 
    name, 
    safe_cast(payment_price as numeric) as payment_price, 
    safe_cast(price as numeric) as price,
    status, 
    safe_cast(mdet_date as date) as mdet_date,
    DATETIME( TIMESTAMP_SECONDS(SAFE_CAST(inserted_ts AS INT64)), "Europe/Budapest" ) AS inserted_ts
from `@@PROJECT_ID@@.raw_arpadent.medrec_details_raw`;
