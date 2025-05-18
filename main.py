from flask import Flask, request, jsonify
from google.auth import default
from google.auth.transport.requests import Request
import requests
import xml.etree.ElementTree as ET
import os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_otm_data():
    content_type = request.content_type
    print("Received Content-Type:", content_type)
    
    if content_type == "application/json":
        otm_payload = request.get_json()
        user_message = otm_payload.get("message", "Hello from JSON!")
    
    elif content_type == "text/xml":
        try:
            xml_data = request.data.decode("utf-8")
            root = ET.fromstring(xml_data)

            # Adjust this depending on actual structure
            message_elem = root.find(".//message")
            user_message = message_elem.text if message_elem is not None else "Hello from XML!"
        except Exception as e:
            return jsonify({"error": "Failed to parse XML", "details": str(e)}), 400
    
    else:
        return jsonify({"error": "Unsupported Content-Type", "details": content_type}), 415

    # Get Google Cloud access token
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    access_token = credentials.token

    # Vertex AI endpoint
    url = "https://us-central1-aiplatform.googleapis.com/v1/projects/utility-grin-433905-t2/locations/us-central1/publishers/google/models/chat-bison:predict"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "instances": [{
            "messages": [
                {"author": "user", "content": user_message}
            ]
        }],
        "parameters": {
            "temperature": 0.2,
            "maxOutputTokens": 256,
            "topP": 0.8,
            "topK": 40
        }
    }

    response = requests.post(url, headers=headers, json=body)
    return jsonify(response.json())

if __name__ == "__main__":
    print("Starting app")
    port = int(os.environ.get("PORT", 8080))
    print("Listening on port", port)
    app.run(host="0.0.0.0", port=port)
