import io
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import storage
from google.cloud import bigquery
import functions_framework
from google.api_core.exceptions import NotFound, PreconditionFailed


# 1. Alapadatok konfigurációja
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["BIGQUERY_DATASET_ID"]

PROJECT_ID = "medcon-prod"
DATASET_ID = "raw_arpadent"


# A feldolgozandó XML fájlok pontos listája a ZIP-en belüli fájlnevek alapján
CSV_FILES_MAPPING = {
    "scheduler.xml": "scheduler_raw",
    "clients.xml": "clients_raw",
    "users.xml": "users_raw",
    "payments.xml": "payments_raw",
    "treatments_medrec.xml": "treatments_medrec_raw",
    "medrec_details.xml": "medrec_details_raw",
    "treatments_medrec_prices.xml": "treatments_medrec_prices_raw",
    "treatments_medrec_groups.xml": "treatments_medrec_groups_raw"
}


# Arpadent XML névterek definíciója
NAMESPACES = {
    "s": "uuid:BDC6E3F0-6DA3-11d1-A2A3-00AA00C14882",
    "dt": "uuid:C2F41010-65B3-11d1-A29F-00AA00C14882",
    "rs": "urn:schemas-microsoft-com:rowset",
    "z": "#RowsetSchema",
}


def already_processed(bq_client, bucket_name, file_name):
    """
    Megnézi, hogy az adott ZIP fájl feldolgozásra került-e már.
    Ez védi a Cloud Function-t az ismételt Eventarc / Cloud Storage triggereléstől.
    """
    sql = """
    SELECT COUNT(*) AS cnt
    FROM `medcon-prod.raw_arpadent.processed_zip_files`
    WHERE bucket_name = @bucket_name
      AND file_name = @file_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("bucket_name", "STRING", bucket_name),
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
        ]
    )

    result = bq_client.query(sql, job_config=job_config).result()
    row = next(result)

    return row.cnt > 0


def mark_as_processed(bq_client, bucket_name, file_name):
    """
    Sikeres feldolgozás után beírja a ZIP fájlt a kontroll táblába.
    MERGE-et használunk, hogy ugyanaz a fájl ne kerülhessen be többször.
    """
    sql = """
    MERGE `medcon-prod.raw_arpadent.processed_zip_files` T
    USING (
      SELECT
        @bucket_name AS bucket_name,
        @file_name AS file_name,
        CURRENT_TIMESTAMP() AS processed_at
    ) S
    ON T.bucket_name = S.bucket_name
       AND T.file_name = S.file_name
    WHEN NOT MATCHED THEN
      INSERT (bucket_name, file_name, processed_at)
      VALUES (S.bucket_name, S.file_name, S.processed_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("bucket_name", "STRING", bucket_name),
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
        ]
    )

    bq_client.query(sql, job_config=job_config).result()


def acquire_processing_lock(bucket, file_name):
    """
    Lock fájl létrehozása a ZIP feldolgozás idejére.
    Egyszerre csak egy function példány tudja létrehozni.
    """
    lock_name = f"_locks/{file_name}.lock"
    lock_blob = bucket.blob(lock_name)

    try:
        lock_blob.upload_from_string(
            "processing",
            if_generation_match=0
        )
        print(f"[LOCK] Feldolgozási lock létrehozva: {lock_name}")
        return True
    except PreconditionFailed:
        print(f"[INFO] Már fut feldolgozás ehhez a ZIP-hez, kihagyva: {file_name}")
        return False


def release_processing_lock(bucket, file_name):
    """
    Lock fájl törlése a feldolgozás végén.
    """
    lock_name = f"_locks/{file_name}.lock"
    lock_blob = bucket.blob(lock_name)

    try:
        lock_blob.delete()
        print(f"[LOCK] Feldolgozási lock törölve: {lock_name}")
    except NotFound:
        print(f"[INFO] Lock már nem létezik: {lock_name}")


@functions_framework.cloud_event
def process_arpadent_zip(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Csak .zip kiterjesztésű fájlokra futunk le
    if not file_name.lower().endswith(".zip"):
        print(f"Figyelmen kívül hagyva: {file_name} (nem .zip)")
        return

    print(f"Új ZIP fájl észlelve: {file_name} a(z) {bucket_name} vödörben. Feldolgozás indul...")

    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Ha már feldolgozott ZIP, azonnal kilépünk
    if already_processed(bq_client, bucket_name, file_name):
        print(f"[INFO] Már feldolgozott ZIP, kihagyva: {file_name}")
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Lock megszerzése: ha már fut egy másik példány erre a ZIP-re, kilépünk
    if not acquire_processing_lock(bucket, file_name):
        return

    try:
        # ZIP fájl letöltése memóriába
        try:
            zip_bytes = blob.download_as_bytes()
        except NotFound:
            print(f"[INFO] A fájl már nem található, kihagyva: gs://{bucket_name}/{file_name}")
            return

        processed_tables_count = 0

        # ZIP kicsomagolása és XML-ek feldolgozása
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            all_files_in_zip = z.namelist()
            print(f"[DEBUG] A ZIP-ben található összes fájl listája: {all_files_in_zip}")

            for inner_path in all_files_in_zip:
                # Csak a fájl nevét nézzük, mappák nélkül
                # pl. "tables/clients.xml" -> "clients.xml"
                inner_file_name = os.path.basename(inner_path)
                inner_file_lowercase = inner_file_name.lower()

                if inner_file_lowercase in CSV_FILES_MAPPING:
                    target_table_name = CSV_FILES_MAPPING[inner_file_lowercase]
                    table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table_name}"

                    print(f"-> Találtunk egy feldolgozandó XML-t: {inner_path} (Cél tábla: {target_table_name})")

                    try:
                        with z.open(inner_path) as xml_file:
                            xml_content = xml_file.read()

                        df = parse_arpadent_xml(xml_content, inner_file_lowercase)

                        if df is not None and not df.empty:
                            job_config = bigquery.LoadJobConfig(
                                write_disposition="WRITE_TRUNCATE",
                                autodetect=True
                            )

                            job = bq_client.load_table_from_dataframe(
                                df,
                                table_id,
                                job_config=job_config
                            )
                            job.result()

                            print(f"   [OK] Sikeresen feltöltve ide: {table_id} ({len(df)} sor)")
                            processed_tables_count += 1
                        else:
                            print(f"   [FIGYELEM] {inner_path} nem tartalmaz érvényes adatot.")

                    except Exception as e:
                        print(f"   [HIBA] Nem sikerült feldolgozni a(z) {inner_path} fájlt: {e}")

        # Ha sikerült legalább egy raw táblát frissíteni, futtatjuk a staging transzformációkat
        if processed_tables_count > 0:
            transformation_success = trigger_sql_transformations(bq_client)

            if transformation_success:
                mark_as_processed(bq_client, bucket_name, file_name)
                print(f"[SIKER] ZIP feldolgozva és megjelölve feldolgozottként: {file_name}")
            else:
                print(
                    f"[HIBA] A ZIP feldolgozása megtörtént, de a staging transzformáció hibázott. "
                    f"Nem jelöljük feldolgozottnak: {file_name}"
                )
        else:
            print("[INFO] Nem történt táblafrissítés, SQL transzformációk kihagyva.")

    finally:
        release_processing_lock(bucket, file_name)


def parse_arpadent_xml(xml_content, filename):
    """
    Kifejezetten az Arpadent XML struktúrájához igazított parsoló.
    A z:row attribútumait gyűjti ki egy Pandas DataFrame-be.
    """
    try:
        root = ET.fromstring(xml_content)
        rows = []

        for row in root.findall(".//z:row", NAMESPACES):
            rows.append(row.attrib)

        return pd.DataFrame(rows)

    except ET.ParseError as e:
        print(f"XML parse hiba a memóriában feldolgozott {filename} fájlnál: {e}")
        return None
    except Exception as e:
        print(f"Általános parsolási hiba ennél a fájlnál: {filename}: {e}")
        return None


def trigger_sql_transformations(bq_client):
    """
    Elindítja a BigQuery transzformációs folyamatot:
    raw_arpadent -> stg_arpadent
    """
    print("Indul az SQL transzformációs lánc a BigQuery-ben...")

    create_staging_tables = """
    create or replace table `{PROJECT_ID}.stg_arpadent.clients` as
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
    from `{PROJECT_ID}.raw_arpadent.clients_raw`;

    create or replace table `{PROJECT_ID}.stg_arpadent.payments` as
    select
    safe_cast(id as int64) as id,
    safe_cast(client_id as int64) as client_id,
    safe_cast(discount_amount as numeric) as discount_amount,
    safe_cast(billed_price_sum as numeric) as billed_price_sum,
    safe_cast(inserted_ts as datetime)        as inserted_ts,
    safe_cast(payment_storno_ts as datetime)  as payment_storno_ts
    from `{PROJECT_ID}.raw_arpadent.payments_raw`
    where billed_price_sum is not null;

    create or replace table `{PROJECT_ID}.stg_arpadent.users` as
    select
    safe_cast(id as int64) as id,
    name,
    doctor,
    safe_cast(inserted_ts as datetime) as inserted_ts
    from `{PROJECT_ID}.raw_arpadent.users_raw`;

    create or replace table `{PROJECT_ID}.stg_arpadent.medrec_details` as
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
    from `{PROJECT_ID}.raw_arpadent.medrec_details_raw`;

    create or replace table `{PROJECT_ID}.stg_arpadent.treatments_medrec` as
    select
    safe_cast(id as int64) as id,
    safe_cast(treatments_medrec_group_id as int64) as treatments_medrec_group_id, 
    short_name,
    name
    from `{PROJECT_ID}.raw_arpadent.treatments_medrec_raw`;

    create or replace table `{PROJECT_ID}.stg_arpadent.scheduler` as
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
    from `{PROJECT_ID}.raw_arpadent.scheduler_raw`;
    """

    try:
        query_job = bq_client.query(create_staging_tables)
        query_job.result()
        print("[SIKER] Az SQL transzformációk sikeresen lefutottak! A Looker Studio adatai frissültek.")
        return True
    except Exception as e:
        print(f"[HIBA] Az SQL transzformációk futtatása meghiúsult: {e}")
        return False