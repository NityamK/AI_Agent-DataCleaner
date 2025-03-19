import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from sqlalchemy import create_engine
from pydantic import BaseModel
import requests
import json

# Ensure the utils folder is in Python's path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from ai_agent import AIAgent  # Import AI Agent
from data_cleaning import DataCleaning  # Import Rule-Based Data Cleaning

app = FastAPI()

# Initialize AI agent and rule-based data cleaner
ai_agent = AIAgent()
cleaner = DataCleaning()

# ------------------------ CSV / Excel Cleaning Endpoint ------------------------

@app.post("/clean-data")
async def clean_data(file: UploadFile = File(...)):
    """Receives file from UI, cleans it using rule-based & AI methods, and returns cleaned JSON."""
    try:
        print("📂 Receiving file:", file.filename)
        contents = await file.read()
        file_extension = file.filename.split(".")[-1]

        # Load file into Pandas DataFrame
        if file_extension == "csv":
            df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
        elif file_extension == "xlsx":
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")

        print("✅ Initial DataFrame Loaded:\n", df.head())

        # Step 1: Rule-Based Cleaning
        df_cleaned = cleaner.clean_data(df)
        print("✅ Rule-Based Cleaning Done")

        # Step 2: AI-Powered Cleaning
        df_ai_cleaned = ai_agent.process_data(df_cleaned)
        print("🧠 AI Processing Output:\n", df_ai_cleaned)

        # 🔴 Extract JSON from AI response (fix)
        try:
            json_start = df_ai_cleaned.find("[")  # Find start of JSON array
            json_end = df_ai_cleaned.rfind("]") + 1  # Find end of JSON array
            cleaned_json_str = df_ai_cleaned[json_start:json_end]

            cleaned_data = json.loads(cleaned_json_str)  # Convert to Python dictionary
            print("✅ Extracted JSON Data:\n", cleaned_data)

            return {"cleaned_data": cleaned_data}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error parsing AI output: {str(e)}")

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


# ------------------------ Database Query Cleaning Endpoint ------------------------

class DBQuery(BaseModel):
    db_url: str
    query: str

@app.post("/clean-db")
async def clean_db(query: DBQuery):
    """Fetches data from a database, cleans it using AI, and returns cleaned JSON."""
    try:
        engine = create_engine(query.db_url)
        df = pd.read_sql(query.query, engine)

        # Step 1: Rule-Based Cleaning
        df_cleaned = cleaner.clean_data(df)

        # Step 2: AI-Powered Cleaning
        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        # Convert AI cleaned data to DataFrame
        if isinstance(df_ai_cleaned, str):
            from io import StringIO
            df_ai_cleaned = pd.read_csv(StringIO(df_ai_cleaned))

        return {"cleaned_data": df_ai_cleaned.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from database: {str(e)}")

# ------------------------ API Data Cleaning Endpoint ------------------------

class APIRequest(BaseModel):
    api_url: str

@app.post("/clean-api")
async def clean_api(api_request: APIRequest):
    """Fetches data from an API, cleans it using AI, and returns cleaned JSON."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_request.api_url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to fetch data from API.")
                
                data = await response.json()
                df = pd.DataFrame(data)

                # Step 1: Rule-Based Cleaning
                df_cleaned = cleaner.clean_data(df)

                # Step 2: AI-Powered Cleaning
                df_ai_cleaned = ai_agent.process_data(df_cleaned)

                # Convert AI cleaned data to DataFrame
                if isinstance(df_ai_cleaned, str):
                    from io import StringIO
                    df_ai_cleaned = pd.read_csv(StringIO(df_ai_cleaned))

                return {"cleaned_data": df_ai_cleaned.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing API data: {str(e)}")

# ------------------------ Run Server ------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
