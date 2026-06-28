# Adatból döntés Cloud Run App

Python alapú Cloud Run alkalmazás az Adatból döntés projekt automatizált adatfeldolgozási feladataihoz.

## Stack

- Python
- Google Cloud Run
- Google Cloud Build
- BigQuery
- Docker

## Deployment

A repository Cloud Build triggerrel kapcsolódik a Google Cloud Run szolgáltatáshoz.
Push esetén a Cloud Build új image-et készít, majd deployolja Cloud Runra.

GitHub push
→ Cloud Build automatikus build
→ Docker image
→ Cloud Run új revision
→ ZIP feltöltés Cloud Storage-ba
→ XML-ek feldolgozása
→ raw_arpadent táblák frissítése
→ sql/staging/*.sql fájlok futtatása
→ stg_arpadent táblák frissítése
→ Google Data Studio friss adatokból dolgozik

## Biztonság

A repository nem tartalmaz ügyféladatot, service account kulcsot vagy környezeti változókat.