from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.auth import default
from google.auth.transport.requests import Request
import xml.etree.ElementTree as ET
import os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_otm_data():
    try:
        # Step 1: Get raw XML data from OTM
        xml_data = request.data
        if not xml_data:
            return jsonify({"error": "No XML data received"}), 400

        # Step 2: Parse XML
        root = ET.fromstring(xml_data)

        # Example: Extract fields from XML (adjust paths based on your XML schema)
        invoice_number = root.findtext(".//InvoiceNumber") or "Unknown"
        amount = root.findtext(".//Amount") or "0.0"

        # Step 3: Prepare data row for BigQuery
        row = {
            "invoice_number": invoice_number,
            "amount": float(amount)
        }

        # Step 4: Create BigQuery client using correct project
        client = bigquery.Client(project="utility-grin-433905-t2")

        # Fully-qualified BigQuery table ID (change to your actual table)
        table_id = "utility-grin-433905-t2.otm_data.invoices"

        # Step 5: Insert data into BigQuery
        errors = client.insert_rows_json(table_id, [row])

        if errors:
            return jsonify({"status": "error", "details": errors}), 500
        else:
            return jsonify({"status": "success", "inserted": row}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("Starting Flask app...")
    port = int(os.environ.get("PORT", 8080))
    print(f"Listening on port {port}")
    app.run(host="0.0.0.0", port=port)

