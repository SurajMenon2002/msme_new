import zipfile
import os
import pandas as pd
from sqlalchemy import create_engine

# === CONFIGURATION ===
ZIP_PATH = "/mnt/data/msme_definitions_by_sector.zip"
EXTRACT_DIR = "/mnt/data/msme_definitions"

DB_CONFIG = {
    "db_name": "final",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432",
    "schema": "public"
}

# === 1. UNZIP FILE ===
def unzip_files(zip_path, extract_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"✅ Extracted files to: {extract_dir}")

# === 2. GET EXCEL FILES ===
def load_excel_files(extract_dir):
    return [f for f in os.listdir(extract_dir) if f.endswith(".xlsx") or f.endswith(".xls")]

# === 3. UPLOAD EACH FILE AS A TABLE ===
def upload_files_as_tables(files, extract_dir, engine, schema):
    uploaded_tables = []
    for file in files:
        file_path = os.path.join(extract_dir, file)
        table_name = os.path.splitext(file)[0].lower().replace(" ", "_").replace("-", "_").replace(".", "_")

        try:
            df = pd.read_excel(file_path)  # Read first/default sheet
            df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
            uploaded_tables.append(table_name)
            print(f"📥 Uploaded '{file}' as table '{table_name}'")
        except Exception as e:
            print(f"❌ Failed to upload '{file}': {e}")
    return uploaded_tables

# === 4. MAIN ===
def main():
    unzip_files(ZIP_PATH, EXTRACT_DIR)

    excel_files = load_excel_files(EXTRACT_DIR)

    # Setup PostgreSQL connection
    engine_str = f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["db_name"]}'
    engine = create_engine(engine_str)

    uploaded_tables = upload_files_as_tables(excel_files, EXTRACT_DIR, engine, DB_CONFIG["schema"])

    print("\n🎉 Upload complete. Tables created:", uploaded_tables)

if __name__ == "__main__":
    main()
