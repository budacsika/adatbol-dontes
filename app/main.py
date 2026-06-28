import io
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataform_v1beta1
import functions_framework
from pathlib import Path
from google.api_core.exceptions import NotFound, PreconditionFailed

# 1. Alapadatok konfigurációja
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["BIGQUERY_DATASET_ID"]
REPOSITORY_ID = os.environ["REPOSITORY_ID"]
REGION = os.environ["REGION"]
DATAFORM_GIT_COMMITISH = os.environ["DATAFORM_GIT_COMMITISH"]
DATAFORM_SERVICE_ACCOUNT = os.environ["DATAFORM_SERVICE_ACCOUNT"]

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
            transformation_success = run_dataform_workflow()

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

def run_dataform_workflow() -> bool:
    print("Indul a Dataform workflow...")
    print(f"PROJECT_ID={PROJECT_ID}")
    print(f"REGION={REGION}")
    print(f"REPOSITORY_ID={REPOSITORY_ID}")
    print(f"DATAFORM_SERVICE_ACCOUNT={DATAFORM_SERVICE_ACCOUNT}")

    try:
        client = dataform_v1beta1.DataformClient()
        repository_path = client.repository_path(PROJECT_ID, REGION, REPOSITORY_ID)

        print(f"Dataform repository_path: {repository_path}")

        print("CompilationResult indul...")
        compilation_result = client.create_compilation_result(
            parent=repository_path,
            compilation_result=dataform_v1beta1.CompilationResult(
                git_commitish="main"
            ),
        )
        print("WorkflowInvocation indul...")
        workflow_invocation = client.create_workflow_invocation(
            parent=repository_path,
            workflow_invocation=dataform_v1beta1.WorkflowInvocation(
                compilation_result=compilation_result.name,
                invocation_config=dataform_v1beta1.InvocationConfig(
                    service_account=DATAFORM_SERVICE_ACCOUNT
                ),
            ),
        )

        print(f"[SIKER] Dataform workflow elindítva: {workflow_invocation.name}")
        return True

    except Exception as e:
        print(f"[HIBA] A Dataform workflow indítása meghiúsult: {type(e)}")
        print(f"[HIBA] Részletes hiba: {repr(e)}")
        return False