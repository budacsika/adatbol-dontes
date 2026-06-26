create or replace table `@@PROJECT_ID@@.stg_arpadent.treatments_medrec` as
select
    safe_cast(id as int64) as id,
    safe_cast(treatments_medrec_group_id as int64) as treatments_medrec_group_id, 
    short_name,
    name
from `@@PROJECT_ID@@.raw_arpadent.treatments_medrec_raw`;
