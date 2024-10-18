import os
import json
import base64
from google.cloud import bigquery
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import GoogleAPIError
import logging

# Konfiguriere das Logging
logging.basicConfig(level=logging.INFO)

def initialize_firestore(project_id):
    """
    Initialisiert die Firestore-Verbindung mit den Application Default Credentials.
    """
    if not firebase_admin._apps:
        logging.info(f"Initialisiere Firestore für Projekt: {project_id}")
        firebase_admin.initialize_app(
            credentials.ApplicationDefault(),
            {
                'projectId': project_id,
            }
        )
    db = firestore.client()
    logging.info(f"Firestore initialisiert für Projekt: {project_id}")
    return db

def fetch_bigquery_data(project_id, query):
    """
    Führt eine benutzerdefinierte Abfrage in BigQuery aus und gibt die Ergebnisse zurück.
    Verwendet die Application Default Credentials für die Authentifizierung.
    """
    logging.info(f"Führe BigQuery-Abfrage im Projekt '{project_id}' aus.")
    client = bigquery.Client(project=project_id)
    query_job = client.query(query)

    try:
        results = query_job.result()  # Warten bis die Abfrage abgeschlossen ist
        logging.info(f"Abfrage abgeschlossen. Anzahl der Zeilen: {results.total_rows}")
        return results
    except GoogleAPIError as e:
        logging.error(f"Fehler bei der BigQuery-Abfrage: {e}")
        return None

def push_to_firestore(db, rows, collection_name, id_field):
    """
    Pusht BigQuery-Daten nach Firestore.
    Jede Zeile verwendet das `id_field` als Dokument-ID und die restlichen Felder als Schlüssel-Wert-Paare.
    """
    batch = db.batch()
    batch_size = 500  # Maximale Batch-Größe für Firestore
    count = 0

    for row in rows:
        data = dict(row)
        key = data.get(id_field)
        if not key:
            logging.warning(f"Datensatz ohne '{id_field}' gefunden: {data}")
            continue

        # Bereinige den Schlüssel, um ungültige Zeichen zu entfernen
        sanitized_key = ''.join(e for e in str(key) if e.isalnum())

        # Entferne das ID-Feld aus den Daten
        data.pop(id_field, None)

        doc_ref = db.collection(collection_name).document(sanitized_key)
        batch.set(doc_ref, data)
        count += 1

        # Committe die Batch, wenn die maximale Größe erreicht ist
        if count % batch_size == 0:
            try:
                batch.commit()
                logging.info(f"{count} Dokumente erfolgreich geschrieben.")
            except GoogleAPIError as e:
                logging.error(f"Fehler beim Schreiben von Batch: {e}")
            batch = db.batch()

    # Committe verbleibende Dokumente
    if count % batch_size != 0:
        try:
            batch.commit()
            logging.info(f"Total {count} Dokumente erfolgreich geschrieben.")
        except GoogleAPIError as e:
            logging.error(f"Fehler beim Schreiben der letzten Batch: {e}")

def load_data(event, context):
    """
    Cloud Function Entry Point.
    Erwartet ein Pub/Sub-Event und führt den Datenimport durch.
    """
    # Optional: Verarbeite die Pub/Sub-Nachricht
    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        logging.info(f"Empfangene Nachricht: {message}")

    # Lade Umgebungsvariablen
    PROJECT_ID = os.getenv("PROJECT_ID")
    BIGQUERY_QUERY = os.getenv("BIGQUERY_QUERY")
    FIRESTORE_COLLECTION = os.getenv("FIRESTORE_COLLECTION")
    FIRESTORE_ID_FIELD = os.getenv("FIRESTORE_ID_FIELD")

    # Überprüfe, ob alle notwendigen Umgebungsvariablen gesetzt sind
    if not all([PROJECT_ID, BIGQUERY_QUERY, FIRESTORE_COLLECTION, FIRESTORE_ID_FIELD]):
        logging.error("Fehlende Umgebungsvariablen. Beende das Skript.")
        return

    # Initialisiere Firestore
    db = initialize_firestore(PROJECT_ID)

    # Fetch BigQuery-Daten
    rows = fetch_bigquery_data(PROJECT_ID, BIGQUERY_QUERY)
    if rows is None:
        logging.error("Keine Daten von BigQuery erhalten. Beende das Skript.")
        return

    # Pushe Daten nach Firestore
    push_to_firestore(db, rows, FIRESTORE_COLLECTION, FIRESTORE_ID_FIELD)
