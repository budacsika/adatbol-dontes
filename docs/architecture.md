# Architektúra

## Áttekintés

Ez a projekt az **Adatból döntés** adatfeldolgozási és üzleti döntéstámogató munkafolyamat része.

Az alkalmazás célja, hogy kis- és középvállalkozások számára támogassa az automatizált adatfeldolgozást, a strukturált adatbetöltést és az elemzésre előkészített BigQuery adattáblák létrehozását.

A jelenlegi megvalósítás egy Python-alapú Cloud Run szolgáltatásra épül, amely forrásadatokat dolgoz fel és tölt be Google BigQuery-be. Az így betöltött adatokból később staging, reporting és dashboard-kész rétegek építhetők.

## Magas szintű architektúra

```text
Forrásadatok
    ↓
Python alkalmazás
    ↓
Google Cloud Run
    ↓
BigQuery raw dataset
    ↓
BigQuery staging / reporting nézetek
    ↓
Looker Studio dashboardok