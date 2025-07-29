import sys
import subprocess
import importlib.util

# List of required packages
required_packages = [
    "pypdfium2",
    "openai"
]
 
# Check if each package is installed, install if missing
for package in required_packages:
    # Check if the package is already installed
    if importlib.util.find_spec(package) is None:
        print(f"{package} not found, attempting to install...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install {package}: {e}")
            print("Please install the required packages manually and retry.")
            sys.exit(1)
# imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
from openai import OpenAI
import base64
from pathlib import Path
import shutil
from multiprocessing import Pool
import pypdfium2 as pdfium
import os
import re

# Settings
DIR_NAMES = ["Bilanz03_EU_neg_EK_kontennachweise"]

CONN_ID = "dsv_ingest"
PDF_FILENAME = "Bilanz03_EU_neg_EK_kontennachweise.pdf"
BASE_PATH="/mnt/datasources/vast/glfsshare/DSV/"
# Define the path for ingest data
INGEST_DIR = "data/ingest/"
# Define the path for jpgs
JPG_DIR = "data/img/"
# Define the path for txts
TXT_DIR = "data/txt/"
#Define the path for txts with tables
TABLE_DIR = "data/txt/tablesonly/"
# Define the path for txts with no table
NOTABLES_DIR = "data/txt/notables/"
# Define the path for merged txts
MERGEDTABLES_DIR = "data/txt/tablesonly/merged_tables/"
# Define the directory to scan for PDFs
PDF_DIR = "data/pdfs"
# Define the .json directories base path
JSON_DIR = "data/json"

# nanonetsOCR
NANONETSOCR_API_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NTMxODkxOTEsImlzcyI6ImFpb2xpQGhwZS5jb20iLCJzdWIiOiJlNWM1ZGRiZC1kMWYxLTRiYmQtODg3YS1iZTg0ZGIyZDUwZWEiLCJ1c2VyIjoiYWRtaW4ifQ.wvyvk26k8Vwndm21jHXELOCwDji8J-73BJFzjm07Ktp6jv4-Sj22xj7GmshQ6f24svRfBonq6YMpa_PilgB55B_STe-hD-Be-c9u-JwxQ0Bp6QUt-bpG86usVtF_HwTUIvgVhEYrx-RSn0Y9_yUN_v9cTIiZKuxXsvRWlqYRs7UA_HUoztQVngkHqwcmA7kKizE0El_K1W9fS8rzVc3WaumMLXZcG5oM2I0zuG4AiiEe3IpvKdVAULiqbxy8v8x6274RJkxWiyXlWWAXiCbs11wRx2etkMPX4ndtaI7DJenDJea9xK3ifPLHgvJeObGO2PcFpAaPaNOJWFwdHhDe1Q"
NANONETSOCR_BASE_URL = "https://nanonets-ocr-s-predictor-dominic-viola-3c5c07d6.ingress.pcai0203.fr2.hpecolo.net/v1"
NANONETSOCR_MODEL = "nanonets/Nanonets-OCR-s"

TAG_RE = re.compile(r'<[^>]+>')

# Create dictionary with tables and wo tables
full_md_content = {}
md_con_only_pages_with_relevant_tables = {}
md_con_only_pages_wo_tables = {}

# Get directory from fs connection
def get_base_path():
    conn = BaseHook.get_connection(CONN_ID)
    path = conn.extra_dejson.get("path")
    if not path:
        raise ValueError(f"Connection '{CONN_ID}' has no 'path' in extra.")
    logging.info(f"Path from '{CONN_ID}' connection: {path}")
    return path

def read_pdf_from_connection():
    path = os.path.join(BASE_PATH, INGEST_DIR)
    #logging.info(f"Path from '{CONN_ID}' connection: {path}")
    logging.info(f"Path to file: {path}")

    # Check if the path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")
        
    pdf_path = os.path.join(path, PDF_FILENAME)
    
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"File not found: {pdf_path}")

    logging.info(f"Path to pdf file: {pdf_path}")
    
    # Read PDF file
    with open(pdf_path, "rb") as f:
            binary_data = f.read()
    
    logging.info("pdf file read succesfully.")

def convert_pdf_into_jpg_by_page():
    pdf_path = os.path.join(BASE_PATH, PDF_DIR)
    jpg_path = os.path.join(BASE_PATH, JPG_DIR)
    
    # Loop over .pdf files in the directory
    for filename in os.listdir(pdf_path):
        if filename.lower().endswith(".pdf"):
            full_path = os.path.join(pdf_path, filename)
            inputdoc = os.path.splitext(os.path.basename(full_path))[0]
    
            # Load a document
            pdf = pdfium.PdfDocument(full_path)
    
            #Create output folder if not existing yet
            out_path=jpg_path + "/" + inputdoc
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            # set permissions to folder
            os.chmod(out_path, 0o777)
        
            # Loop over pages and render
            for i in range(len(pdf)):
                page = pdf[i]
                image = page.render().to_pil()
                image.save(f"{out_path}/page{i:03d}.jpg")
                # set permissions to file
                os.chmod(f"{out_path}/page{i:03d}.jpg", 0o666)

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")

def ocr_page_with_nanonets_s(img_base64):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{img_base64}"},
                    },
                    {
                        "type": "text",
                        "text": "Extract the text from the above document as if you were reading it naturally. Be aware that the tables sometimes don't have border or grid lines. Return the tables in html format. Return the equations in LaTeX representation. If there is an image in the document and image caption is not present, add a small description of the image inside the <img></img> tag; otherwise, add the image caption inside <img></img>. Watermarks should be wrapped in brackets. Ex: <watermark>OFFICIAL COPY</watermark>. Page numbers should be wrapped in brackets. Ex: <page_number>14</page_number> or <page_number>9/22</page_number>. Prefer using ☐ and ☑ for check boxes.",
                    },
                ],
            }
        ],
        temperature=0.0,
        max_tokens=8000
    )
    return response.choices[0].message.content

def remove_tags(text):
    return TAG_RE.sub('', text)

def apply_processing(triple_tuple):
    txt_path = os.path.join(BASE_PATH, TXT_DIR)
    table_path = os.path.join(BASE_PATH, TABLE_DIR)
    notables_path = os.path.join(BASE_PATH, NOTABLES_DIR)
    
    img_fullpath = triple_tuple[0]
    dir_name = triple_tuple[1]


    full_md_content[dir_name] = {}
    md_con_only_pages_with_relevant_tables[dir_name] = {}
    md_con_only_pages_wo_tables[dir_name] = {}
    
    img = triple_tuple[2]
    page = img.replace(".jpg","")

    img_base64 = encode_image(img_fullpath)
    print(f"Processing {img_fullpath}...")
    md_output = ocr_page_with_nanonets_s(img_base64)
    full_md_content[dir_name][page] = md_output

    #create folder if it does not exist and set permisssions
    output_path = Path(os.path.join(txt_path,dir_name))
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        os.chmod(output_path, 0o777)
    tableout_path = Path(os.path.join(table_path,dir_name))
    if not os.path.exists(tableout_path):
        os.makedirs(tableout_path)
        os.chmod(tableout_path, 0o777)
    notablesout_path = Path(os.path.join(notables_path,dir_name))
    if not os.path.exists(notablesout_path):
        os.makedirs(notablesout_path)
        os.chmod(notablesout_path, 0o777)

    #create file and write markdown content to it
    with open(f"{output_path}/{page}.txt", "w") as file:
        file.write(full_md_content[dir_name][page])

            #identify if there's a relevant table
    if "<table>" in full_md_content[dir_name][page]:
        print("found a table at ", page)
        if "Inhaltsverzeichnis" in full_md_content[dir_name][page]:
            print("found a non relevant table at ",page)
        else:
            md_con_only_pages_with_relevant_tables[dir_name][page] = md_output
            with open(f"{tableout_path}/{page}.txt", "w") as file:
                file.write(full_md_content[dir_name][page])
            
    else:
        print("no table here at ", page)
        md_con_only_pages_wo_tables[dir_name][page] = md_output
        with open(f"{notablesout_path}/{page}.txt", "w") as file:
            file.write(full_md_content[dir_name][page])
    
    return (dir_name, img, full_md_content[dir_name][page])

def process_img_to_markdown():
    # here all image files to process are gathered
    all_files_to_process = []
    for dir_name in DIR_NAMES:
        dir_fullpath = os.path.join(JPG_DIR,dir_name)
        assert os.path.isdir(dir_fullpath)
        
        imgs = os.listdir(dir_fullpath)
        for img in imgs:
            if os.path.isdir(os.path.join(dir_fullpath, img)):
                print(f"{img} is a dir, skip it")
            else:
                img_fullpath = os.path.join(dir_fullpath,img)
                all_files_to_process.append((img_fullpath, dir_name, img))
                
    with Pool(8) as p:
        results = p.map(apply_processing, all_files_to_process)

# Define DAG
with DAG(
    dag_id="dsv_ocr_workflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual or sensor-based
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    description="Run the OCR workflow."
) as dag:

    read_pdf_task = PythonOperator(
        task_id="read_pdf_file",
        python_callable=read_pdf_from_connection
    )

    split_pdf_to_jpg = PythonOperator(
        task_id="split_pdf_to_jpg",
        python_callable=convert_pdf_into_jpg_by_page
    )

    convert_jpg_to_markdown = PythonOperator(
        task_id="convert_jpg_to_markdown",
        python_callable=process_img_to_markdown
    )
    
    split_pdf_to_jpg >> convert_jpg_to_markdown
