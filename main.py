from flask import Flask, request, jsonify
from google.auth import default
from google.auth.transport.requests import Request
import requests

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_otm_data():
    # Get the JSON payload from OTM or another external system
    otm_payload = request.get_json()
    user_message = otm_payload.get("message", "Hello!")

    # Get a valid Google Cloud access token
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    access_token = credentials.token

    # Set the Vertex AI chat-bison endpoint
    url = "https://us-central1-aiplatform.googleapis.com/v1/projects/utility-grin-433905-t2/locations/us-central1/publishers/google/models/chat-bison:predict"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Create the request body for the chat model
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

    # Make the request to Vertex AI
    response = requests.post(url, headers=headers, json=body)

    # Return the Vertex AI response
    return jsonify(response.json())

if _name_ == "_main_":
    app.run(host="0.0.0.0", port=8080)
