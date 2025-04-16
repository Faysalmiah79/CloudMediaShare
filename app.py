from flask import Flask, request, jsonify
import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient, PartitionKey
import uuid

app = Flask(__name__)

# --- Azure Configuration (replace with your values or use environment variables) ---
AZURE_BLOB_CONNECTION_STRING = os.environ.get("AZURE_BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "mediafiles"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION_STRING)

COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DATABASE = "MediaDB"
COSMOS_CONTAINER = "MediaMetadata"

# Initialize Cosmos DB client
cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.create_database_if_not_exists(id=COSMOS_DATABASE)
container = database.create_container_if_not_exists(
    id=COSMOS_CONTAINER,
    partition_key=PartitionKey(path="/userId"),
    offer_throughput=400
)

# --- API Endpoints ---

@app.route('/api/media', methods=['POST'])
def upload_media():
    file = request.files.get('file')
    title = request.form.get('title', 'Untitled')
    user_id = request.form.get('userId', 'default_user')
    
    if not file:
        return jsonify({"error": "No file provided"}), 400
        
    # Generate unique blob name
    blob_name = f"{uuid.uuid4()}_{file.filename}"
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
    
    try:
        blob_client.upload_blob(file, overwrite=True, content_settings=ContentSettings(content_type=file.content_type))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Save metadata in Cosmos DB
    metadata = {
        "id": str(uuid.uuid4()),
        "userId": user_id,
        "title": title,
        "blobName": blob_name,
        "fileName": file.filename
    }
    container.create_item(body=metadata)

    return jsonify({"message": "Upload successful", "metadata": metadata}), 201

@app.route('/api/media', methods=['GET'])
def list_media():
    media_items = list(container.read_all_items())
    return jsonify(media_items), 200

@app.route('/api/media/<string:item_id>', methods=['PUT'])
def update_media(item_id):
    data = request.get_json()
    try:
        # Get the item first (for simplicity, assuming item exists)
        item = container.read_item(item=item_id, partition_key=data.get('userId', 'default_user'))
        item.update(data)
        container.upsert_item(item)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"message": "Update successful"}), 200

@app.route('/api/media/<string:item_id>', methods=['DELETE'])
def delete_media(item_id):
    try:
        # For demo, retrieve and delete using partition key (you could store userId in metadata)
        item = container.read_item(item=item_id, partition_key='default_user')
        container.delete_item(item, partition_key='default_user')
        # Also delete the blob file here (not implemented in full for brevity)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"message": "Deletion successful"}), 200

if __name__ == '__main__':
    app.run(debug=True)
