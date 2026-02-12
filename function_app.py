import azure.functions as func
import logging
import os
import csv
import io
import pymongo
import certifi
from datetime import datetime

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="reviews/{phopstoragegroup}",
                  connection="AzureWebJobsStorage")
def reviews_blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    # Check if the file is a CSV
    if not myblob.name.endswith('.csv'):
        logging.warning(f"File {myblob.name} is not a CSV. Skipping.")
        return

    try:
        # Read the blob content
        blob_content = myblob.read().decode('utf-8')
        csv_file = io.StringIO(blob_content)
        reader = csv.DictReader(csv_file)
        
        # Connect to MongoDB
        mongo_uri = os.environ.get("MONGO_URI")
        db_name = os.environ.get("MONGO_DB_NAME")
        
        if not mongo_uri or not db_name:
            logging.error("MONGO_URI or MONGO_DB_NAME environment variables are not set.")
            return

        client = pymongo.MongoClient(mongo_uri, tlsCAFile=certifi.where())
        db = client[db_name]
        collection = db['reviews']
        
        reviews_to_insert = []
        for row in reader:
            try:
                review = {
                    'product_id': int(row['product_id']),
                    'user_id': int(row['user_id']),
                    'rating': int(row['rating']),
                    'comment': str(row['comment']),
                    'created_at': datetime.now().isoformat()
                }
                reviews_to_insert.append(review)
            except (ValueError, KeyError) as e:
                logging.error(f"Error parsing row: {row}. Error: {e}")
                continue
        
        if reviews_to_insert:
            result = collection.insert_many(reviews_to_insert)
            logging.info(f"Successfully inserted {len(result.inserted_ids)} reviews into MongoDB.")
        else:
            logging.info("No valid reviews found to insert.")
            
    except Exception as e:
        logging.error(f"Error processing blob {myblob.name}: {e}")
