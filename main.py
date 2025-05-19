from flask import Flask, request, jsonify
from google.cloud import bigquery
import xml.etree.ElementTree as ET
import os
import traceback

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
            print("Reading Content")
            xml_data = request.data.decode("utf-8")
            root = ET.fromstring(xml_data)
            message_elem = root.find(".//message")
            print("Reading Message Element ", message_elem)
            
            user_message = message_elem.text if message_elem is not None else "Hello from XML!"
            print("User Message", user_message)
        except Exception as e:
            print("XML parse error:", traceback.format_exc())
            return jsonify({"error": "Failed to parse XML", "details": str(e)}), 400
    else:
        return jsonify({"error": "Unsupported Content-Type", "details": content_type}), 415

    try:
        # Initialize BigQuery client with explicit project
        client = bigquery.Client(project="utility-grin-433905-t2")
        print("BigQuery client initialized:", client)

        # Use fully qualified table ID string
        table_id = "utility-grin-433905-t2.fleet_maintenance_forecasting.invoice_status"
        print("Using table ID:", table_id)

        # Define rows to insert
        rows_to_insert = [{"message": user_message}]

        # Insert rows
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            print("BigQuery insert errors:", errors)
            return jsonify({"error": "Failed to insert into BigQuery", "details": errors}), 500
        else:
            print("Inserted rows:", rows_to_insert)
            return jsonify({"status": "success", "inserted": rows_to_insert})

    except Exception as e:
        print("Exception during BigQuery insert:", traceback.format_exc())
        return jsonify({"error": "Exception during BigQuery insert", "details": str(e)}), 500


if __name__ == "__main__":
    print("Starting Flask app...")
    port = int(os.environ.get("PORT", 8080))
    print(f"Listening on port: {port}")
    app.run(host="0.0.0.0", port=port)
