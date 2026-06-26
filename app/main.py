import io
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import storage
from google.cloud import bigquery
import functions_framework
from pathlib import Path
from google.api_core.exceptions import NotFound, PreconditionFailed

# 1. Alapadatok konfigurációja
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["BIGQUERY_DATASET_ID"]

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

    base_dir = Path(__file__).resolve().parent.parent
    sql_dir = base_dir / "sql" / "staging"

    sql_files = [
        "01_create_clients.sql",
        "02_create_payments.sql",
        "03_create_users.sql",
        "04_create_medrec_details.sql",
        "05_create_treatments_medrec.sql",
        "06_create_scheduler.sql",
    ]

    try:
        for sql_file in sql_files:
            sql_path = sql_dir / sql_file

            print(f"Futtatás indul: {sql_path}")

            sql = sql_path.read_text(encoding="utf-8")
            sql = sql.replace("@@PROJECT_ID@@", PROJECT_ID)

            query_job = bq_client.query(sql)
            query_job.result()

            print(f"[SIKER] Lefutott: {sql_file}")

        print("[SIKER] Az összes SQL transzformáció sikeresen lefutott! A Looker Studio adatai frissültek.")
        return True

    except Exception as e:
        print(f"[HIBA] Az SQL transzformációk futtatása meghiúsult: {e}")
        return False
