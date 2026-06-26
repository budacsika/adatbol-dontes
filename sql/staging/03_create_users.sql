create or replace table `@@PROJECT_ID@@.stg_arpadent.users` as
select
    safe_cast(id as int64) as id,
    name,
    doctor,
    safe_cast(inserted_ts as datetime) as inserted_ts
from `@@PROJECT_ID@@.raw_arpadent.users_raw`;