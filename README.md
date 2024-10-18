# Cloud Function: BigQuery to Firestore

## Überblick
Diese Cloud Function importiert Daten aus einer BigQuery-Abfrage und speichert sie in einer Firestore-Datenbank. Sie wird durch ein Pub/Sub-Event ausgelöst und erfordert die Konfiguration von Umgebungsvariablen für den Zugriff auf BigQuery und Firestore.

## Voraussetzungen
- Google Cloud Projekt mit aktivierten Diensten für:
  - BigQuery API
  - Firestore API
  - Pub/Sub API
- Service Account mit den entsprechenden Rollen:
  - `roles/bigquery.user`
  - `roles/datastore.user`
- Firebase Admin SDK für Firestore-Authentifizierung

## Umgebungsvariablen

| Variable               | Beschreibung                                                                                   |
|------------------------|------------------------------------------------------------------------------------------------|
| `PROJECT_ID`            | Die Google Cloud Projekt-ID, die für BigQuery und Firestore verwendet wird.                    |
| `BIGQUERY_QUERY`        | Die SQL-Abfrage, die in BigQuery ausgeführt wird, um die Daten abzurufen.                       |
| `FIRESTORE_COLLECTION`  | Der Name der Firestore-Sammlung, in der die BigQuery-Daten gespeichert werden sollen.           |
| `FIRESTORE_ID_FIELD`    | Der Name des Feldes, das als Dokument-ID in Firestore verwendet wird (z. B. eine eindeutige ID).|

## Funktionsweise

1. **Pub/Sub-Trigger**: 
   - Die Funktion wird durch eine Pub/Sub-Nachricht ausgelöst. Die empfangene Nachricht wird für Logging-Zwecke verarbeitet.

2. **Umgebungsvariablen laden**: 
   - Die Umgebungsvariablen `PROJECT_ID`, `BIGQUERY_QUERY`, `FIRESTORE_COLLECTION` und `FIRESTORE_ID_FIELD` müssen definiert sein.

3. **Firestore initialisieren**: 
   - Die Firestore-Verbindung wird mit den Application Default Credentials initialisiert.

4. **BigQuery-Daten abrufen**: 
   - Die in `BIGQUERY_QUERY` definierte Abfrage wird in BigQuery ausgeführt. Die Ergebnisse werden zurückgegeben.

5. **Daten in Firestore speichern**: 
   - Die abgerufenen Daten werden in die angegebene Firestore-Sammlung `FIRESTORE_COLLECTION` eingefügt. 
   - Das Feld, das in `FIRESTORE_ID_FIELD` angegeben ist, wird als Dokument-ID verwendet.

6. **Batch-Commit**: 
   - Firestore unterstützt das Schreiben in Batches (max. 500 Dokumente pro Batch). Die Funktion verwaltet dies automatisch.

## Installation und Deployment

1. Cloud Function bereitstellen:
   ```bash
   gcloud functions deploy load_data \
     --runtime python310 \
     --trigger-topic YOUR_PUBSUB_TOPIC \
     --set-env-vars PROJECT_ID=your-project-id,BIGQUERY_QUERY="SELECT * FROM your_table",FIRESTORE_COLLECTION=your_collection,FIRESTORE_ID_FIELD=id_field \
     --timeout 540s \
     --region YOUR_REGION
