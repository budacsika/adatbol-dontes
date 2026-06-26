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

## Biztonság

A repository nem tartalmaz ügyféladatot, service account kulcsot vagy környezeti változókat.