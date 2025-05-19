from flask import Flask, request, jsonify
from google.cloud import bigquery
import xml.etree.ElementTree as ET
import os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_otm_data():
    content_type = request.content_type
    print("Received Content-Type:", content_type)

    # Parse message based on content type
    if content_type == "application/json":
        otm_payload = request.get_json()
        user_message = otm_payload.get("message", "Hello from JSON!")

    elif content_type == "text/xml":
        try:
            xml_data = request.data.decode("utf-8")
            root = ET.fromstring(xml_data)
            message_elem = root.find(".//message")
            user_message = message_elem.text if message_elem is not None else "Hello from XML!"
        except Exception as e:
            return jsonify({"error": "Failed to parse XML", "details": str(e)}), 400
    else:
        return jsonify({"error": "Unsupported Content-Type", "details": content_type}), 415

    try:
        # Initialize BigQuery client with correct project ID
        client = bigquery.Client(project="utility-grin-433905-t2")

        # Prepare table reference
        dataset_id = "utility-grin-433905-t2.fleet_maintenance_forecasting"         # 🔁 Replace this
        table_id = "utility-grin-433905-t2.fleet_maintenance_forecasting.invoice_status"             # 🔁 Replace this
        table_ref = client.dataset(dataset_id).table(table_id)

        # Define the row to insert
        rows_to_insert = [{"message": user_message}]

        # Insert the row
        errors = client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            return jsonify({"error": "Failed to insert into BigQuery", "details": errors}), 500
        else:
            return jsonify({"status": "success", "inserted": rows_to_insert})

    except Exception as e:
        return jsonify({"error": "Exception during BigQuery insert", "details": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

