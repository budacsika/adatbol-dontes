create or replace table `@@PROJECT_ID@@.stg_arpadent.scheduler` as
select
    safe_cast(id as int64) as id,
    safe_cast(room_id as int64) as room_id,
    safe_cast(client_id as int64) as client_id,
    safe_cast(user_id as int64) as user_id,
    safe_cast(datetime_from as datetime) as datetime_from,
    safe_cast(datetime_to as datetime) as datetime_to,
    safe_cast(flag_new_client as int64) as flag_new_client,
    safe_cast(flag_old_client as int64) as flag_old_client,
    safe_cast(flag_not_come as int64) as flag_not_come,
    safe_cast(flag_came as int64) as flag_came,
    safe_cast(flag_inside as int64) as flag_inside,
    safe_cast(flag_left as int64) as flag_left,
    safe_cast(flag_treatment as int64) as flag_treatment,
    treatment_note,
    safe_cast(last_modified_ts as datetime) as last_modified_ts,
    safe_cast(update_user_id as int64) as update_user_id,
    safe_cast(inserted_ts as datetime) as inserted_ts,
    safe_cast(inserted_by_user_id as int64) as inserted_by_user_id,
    safe_cast(cancelled_ts as datetime) as cancelled_ts,
    safe_cast(cancelled_doctor_ts as datetime) as cancelled_doctor_ts
from `@@PROJECT_ID@@.raw_arpadent.scheduler_raw`;