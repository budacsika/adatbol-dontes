create or replace table `@@PROJECT_ID@@.stg_arpadent.clients` as
select
    safe_cast(id as int64) as client_id,
    safe_cast(doctor_user_id as int64) as doctor_user_id,
    sex,
    safe_cast(birthday as date) as birthday,
    city,
    country,
    trim(zip) as zip_code,
    safe_cast(inserted_ts as datetime) as inserted_ts,
    safe_cast(updated_ts as datetime) as updated_ts
from `@@PROJECT_ID@@.raw_arpadent.clients_raw`;