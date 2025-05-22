from flask import Flask, request, jsonify
from google.cloud import bigquery
import xml.etree.ElementTree as ET
import os

app = Flask(__name__)

def extract_invoice_fields(xml_data):
    try:
        root = ET.fromstring(xml_data)
        ns = {'otm': 'http://xmlns.oracle.com/apps/otm/transmission/v6.4'}

        # Extract invoice_id
        invoice_id_node = root.find('.//otm:InvoiceGid/otm:Gid/otm:Xid', ns)
        invoice_id = invoice_id_node.text if invoice_id_node is not None else None

        # Extract domain_name
        domain_name_node = root.find('.//otm:InvoiceGid/otm:Gid/otm:DomainName', ns)
        domain_name = domain_name_node.text if domain_name_node is not None else None

        # Extract invoice_num
        invoice_num_node = root.find('.//otm:InvoiceNum', ns)
        invoice_num = invoice_num_node.text if invoice_num_node is not None else None

        return {
            "invoice_id": invoice_id,
            "invoice_num": invoice_num,
            "domain_name": domain_name
        }
    except Exception as e:
        print("XML parsing error:", str(e))
        return None

@app.route("/", methods=["POST"])
def handle_otm_data():
    content_type = request.content_type
    print("Received Content-Type:", content_type)

    if content_type != "text/xml":
        return jsonify({"error": "Unsupported Content-Type", "details": content_type}), 415

    try:
        xml_data = request.data.decode("utf-8")
        invoice_data = extract_invoice_fields(xml_data)
        print("Extracted Invoice Data:", invoice_data)

        if not invoice_data or not invoice_data["invoice_id"]:
            return jsonify({"error": "Required invoice fields missing"}), 400

        # Initialize BigQuery client
        client = bigquery.Client(project="utility-grin-433905-t2")
        table_id = "utility-grin-433905-t2.fleet_maintenance_forecasting.invoice"
        print("BigQuery table:", table_id)

        # Insert into BigQuery
        errors = client.insert_rows_json(table_id, [invoice_data])

        if errors:
            print("BigQuery insert errors:", errors)
            return jsonify({"error": "Failed to insert into BigQuery", "details": errors}), 500
        else:
            return jsonify({"status": "success", "inserted": invoice_data})

    except Exception as e:
        print("Exception:", str(e))
        return jsonify({"error": "Exception during processing", "details": str(e)}), 500

if __name__ == "__main__":
    print("Starting Flask app...")
    port = int(os.environ.get("PORT", 8080))
    print("Listening on port:", port)
    app.run(host="0.0.0.0", port=port)

