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
from airflow.models import Variable
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
#Variable.set("DIR_NAMES", [])
DIR_NAMES = [] 

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
NANONETSOCR_CLIENT = OpenAI(api_key=NANONETSOCR_API_TOKEN, base_url=NANONETSOCR_BASE_URL)

LLAMA_API_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NTI1ODQ5NDgsImlzcyI6ImFpb2xpQGhwZS5jb20iLCJzdWIiOiI1ODUyZDhmZS1hMzg4LTQ1NGUtOTVhNS1mYWE1NzRiMGU2MTkiLCJ1c2VyIjoiYWRtaW4ifQ.MPMEYc42tEZIU2OJxV-P6NIuF45EDLTa-G6b-O69YMae39bnpDHn1Uw5onVvHyp_VpGC3TF79ZImhUxKkRtSVNQplePdhdUonl12ttFDAOD_-pGBhxg6vibcE9h_oDLLvRKaUHgL9WsqFmcmxsjDqsO6Ssze3EiOAzN8aLYmslWLlwJgBcv-fTBPWcR4EJdpYePAjIvG3jEu6W7qv4ZWxlKhoTkfPW0jb8ogxObRz7pioXYu0fstAoPoGIx4_KOi0IsnhT7TU0fjpSIr7mpLUGR_sO6fW1rGS4dZoRDtpawc3NDxndvgNh2GL2r-jQo7WJZiLJX8cGpYWb7SGLtV-Q"
LLAMA_BASE_URL = "https://llama-3-1-8b-instruct-predictor-isabelle-steinh-7e6f8d66.ingress.pcai0203.fr2.hpecolo.net/v1"
LLAMA_MODEL = "meta-llama/Llama-3.1-8B-Instruct"
LLAMA_CLIENT = OpenAI(api_key=LLAMA_API_TOKEN, base_url=LLAMA_BASE_URL)

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

def prep_environment(**context):
    #create folder if it does not exist and set permisssions
    ingest_path = os.path.join(BASE_PATH, INGEST_DIR)
    jpg_path = os.path.join(BASE_PATH, JPG_DIR)
    txt_path = os.path.join(BASE_PATH, TXT_DIR)
    table_path = os.path.join(BASE_PATH, TABLE_DIR)
    notables_path = os.path.join(BASE_PATH, NOTABLES_DIR)
    mergedtables_path = os.path.join(BASE_PATH, MERGEDTABLES_DIR)
    pdf_path = os.path.join(BASE_PATH, PDF_DIR)
    json_path = os.path.join(BASE_PATH, JSON_DIR)
    
    all_paths = [
        ingest_path,
        jpg_path,
        txt_path,
        table_path,
        notables_path,
        mergedtables_path,
        pdf_path,
        json_path
    ]
    for path in all_paths:
        logging.info(f"checking Path: {path}.")
        if not os.path.exists(path):
            os.makedirs(path)
            os.chmod(path, 0o777)
            logging.info(f"Path: {path} created.")

    # extract filename from DAG conf
    filename = context["dag_run"].conf.get("filename")
    files = []
    files.append(filename)
    logging.info(f"List of files: {files}")
    Variable.set("DIR_NAMES", files)
    logging.info(Variable.get("DIR_NAMES"))
    dn = Variable.get("DIR_NAMES")
    logging.info(f"DAG running with filenames: {dn}")


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

def convert_pdf_into_jpg_by_page(**context):
    pdf_path = os.path.join(BASE_PATH, PDF_DIR)
    jpg_path = os.path.join(BASE_PATH, JPG_DIR)
    
    DIR_NAMES = []
    DIR_NAMES.append(context["dag_run"].conf.get("filename"))
    logging.info(f"Conversion running with filename: {DIR_NAMES}")
    
    # Loop over .pdf files in the directory
    #for filename in os.listdir(pdf_path):
    for filename in DIR_NAMES:
        logging.info(f"Converting pdf zo jpg for file: {filename}")
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
    client = NANONETSOCR_CLIENT
    response = client.chat.completions.create(
        model=NANONETSOCR_MODEL,
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

def process_img_to_markdown(**context):
    DIR_NAMES = []
    DIR_NAMES.append((context["dag_run"].conf.get("filename")).stem)
    logging.info(f"Conversion running with filename: {DIR_NAMES}")
    jpg_path = os.path.join(BASE_PATH, JPG_DIR)
    # here all image files to process are gathered
    all_files_to_process = []
    for dir_name in DIR_NAMES:
        dir_fullpath = os.path.join(jpg_path,dir_name)
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

def combine_pages(pages_folder, merged_folder):
    # Get a sorted list of file names in the folder
    file_names = sorted([file for file in os.listdir(pages_folder) if file.endswith('.txt')])
    # Initialize a list to store the entries for each table
    tables = []
    prev_number = None
    # Iterate over the files and combine their contents if they belong to the same table
    current_table = []
    for file_name in file_names:
        page = file_name.strip("page")
        page_number = int(page.strip(".txt"))
        # if it's the first page of the table append it
        if not current_table:
            # if it's the following page append the page to the current list
            if prev_number is not None and int(page_number) == (prev_number + 1):
                current_table.append(int(page_number))
            # if it's not the following page append the list to tables and create new list
            else:
                if current_table:
                    tables.append(current_table)
                current_table = []
            current_table.append(int(page_number))
        else:
            # if it's the following page append the page to the current list
            if int(page_number) == (prev_number + 1):
                current_table.append(int(page_number))
            # if it's not the following page append the list to tables and create new list
            else:
                tables.append(current_table)
                current_table = []
                current_table.append(int(page_number))
        prev_number = page_number
        print("current list is empty" if len(current_table) == 1 else "current list is not empty")

    # Append the last table to the tables list
    if current_table:
        tables.append(current_table)

    # write files for each merged table
    for i, sublist in enumerate(tables):
        merged_table = str(merged_folder) + "/" + "_".join(map(str, sublist)) + ".txt"
        with open(merged_table, "w") as f:
            for item in sublist:
                with open(f"{pages_folder}/page{item:03d}.txt", "r") as page_file:
                    f.write(page_file.read())

def combine_realated_pages(**context):
    DIR_NAMES = []
    DIR_NAMES.append(context["dag_run"].conf.get("filename"))
    logging.info(f"Conversion running with filename: {DIR_NAMES}")
    table_path = os.path.join(BASE_PATH, TABLE_DIR)
    mergedtables_path = os.path.join(BASE_PATH, MERGEDTABLES_DIR)
    #run through all tablesonly 
    for dir in DIR_NAMES:
        key = dir
        table_full_path = Path(os.path.join(table_path,key))
        mergedout_path = Path(os.path.join(mergedtables_path,key))
    
        if not os.path.exists(table_full_path):
            os.makedirs(table_full_path)
            os.chmod(table_full_path, 0o777)
    
        if not os.path.exists(mergedout_path):
            os.makedirs(mergedout_path)
            os.chmod(mergedout_path, 0o777) 
        combine_pages(table_full_path,mergedout_path)

def mergetables_to_json_with_llama(file_path):
    with open(file_path, 'r') as table:
        file_content = table.read()

    client = LLAMA_CLIENT

    # here the json_template LLM should fill is defined
    json_template = """{
      "metadata": {
          "dokumentname": "",
          "berichtstichtag": "",
          "währung": "",
          "skalierung": ["EUR","TEUR","MEUR"]
      },
      "bilanzpositionen": { 
          {
              "positionsname": "",
              "wert": float,
              "wert_vorjahr": float,
              "berichtsteil": ["aktiva", "passiva","guv"],
              "seitenzahl": int
          }
      }
      "kontonachweise": {
          { 
              "hauptkategorie": "",
              "unterkategorie": "",
              "abschnittsname": "",
              "kontoname": "",
              "kontonummer": string,
              "betrag": float,
              "betrag_vorjahr": float,
              "berichtsteil": ["aktiva", "passiva","guv"],
              "seitenzahl": int
          }
      }
    }"""
    
    completion = client.chat.completions.create(
        model=LLAMA_MODEL,
        messages= [
                    {
                        "role": "user",
                        "content": f"Ich habe ein Markdown File mit ein oder mehreren Tabellen darin. ich bin mir nicht sicher ob sie zu einer großen tabelle zusammengehören. was meinst du? {file_content}"
                    },
                    {
                        "role": "user",
                        "content": f"""Kannst du die Tabellen Einträge in ein oder mehrere JSONs überführen, je nachdem ob es sich um eine oder mehrere Tabellen handelt? 
                        Beachte dafür bitte folgende json Struktur Regeln: 
                        1. Verwende beim Befüllen für metadata.skalierung ausschließlich eine der folgenden Optionen 'EUR', 'TEUR' oder 'MEUR'. Dabei steht 'TEUR' für 'Tausend Euro', 'MEUR' steht für 'Tausend Euro' und 'EUR' bedeutet einfach nur 'Euro' ohne Skalierung. Diese Begriffe könnten in verschiedenen Schreibweisen wie zum Beispiel 'TEURO', 'teuro' oder 'Meur' im Text oder der Tabelle vorkommen.
                        2. Verwende für die Bennennung der Währung die ISO-Währungscodes also zum Beispiel 'EUR' statt 'euro' oder 'USD' statt 'US-Dollar'. Lasse hierbei die Skalierung weg. Also statt 'TEUR' nutze nur 'EUR'. 
                        3. Nutze für den Berichtsteil ausschließlich eine der folgenden Optionen: 'aktiva', 'passiva' oder 'GuV'. Entscheide hierbei nach den Wörtern die du in der Tabelle oder im Text findest.
                        
                        Das ist die JSON Struktur an die du dich halten musst und die Ausgabe soll nur JSON enthalten:
                        {json_template} 
                        """
                    }
        ],
        response_format= { "type": "json_object" }
    )

    return completion.choices[0].message.content

def apply_json_processing(triple_tuple):
    json_path = os.path.join(BASE_PATH, JSON_DIR)
    merged_table_fullpath = triple_tuple[0]
    dir_name = triple_tuple[1]
    
    merged_txt = triple_tuple[2]
    pages = merged_txt.replace(".txt","")

    print(f"Processing {merged_table_fullpath}...")
    output = mergetables_to_json_with_llama(merged_table_fullpath)
    print(output)
       # create folder if it does not exist and set permisssions
    output_path = Path(os.path.join(json_path,dir_name))
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        os.chmod(output_path, 0o777)
    with open(f"{output_path}/{pages}.json", "w") as file:
        file.write(output)

def convert_merged_tables_to_json(**context):
    DIR_NAMES = []
    DIR_NAMES.append(context["dag_run"].conf.get("filename"))
    logging.info(f"Conversion running with filename: {DIR_NAMES}")
    mergedtables_path = os.path.join(BASE_PATH, MERGEDTABLES_DIR)
    # here all merged txt files to process are gathered
    all_merged_txt_to_process = []
    for dir_name in DIR_NAMES:
        dir_fullpath = os.path.join(mergedtables_path,dir_name)
        assert os.path.isdir(dir_fullpath)
        
        merged_txts = os.listdir(dir_fullpath)
        for merged_txt in merged_txts:
            if os.path.isdir(os.path.join(dir_fullpath, merged_txt)):
                print(f"{merged_txt} is a dir, skip it")
            else:
                merged_table_fullpath = os.path.join(dir_fullpath,merged_txt)
                all_merged_txt_to_process.append((merged_table_fullpath, dir_name, merged_txt))
    with Pool(8) as p:
        results = p.map(apply_json_processing, all_merged_txt_to_process)

def clean_environment():
    ingest_path = os.path.join(BASE_PATH, INGEST_DIR)
    jpg_path = os.path.join(BASE_PATH, JPG_DIR)
    txt_path = os.path.join(BASE_PATH, TXT_DIR)
    table_path = os.path.join(BASE_PATH, TABLE_DIR)
    notables_path = os.path.join(BASE_PATH, NOTABLES_DIR)
    mergedtables_path = os.path.join(BASE_PATH, MERGEDTABLES_DIR)
    pdf_path = os.path.join(BASE_PATH, PDF_DIR)
    json_path = os.path.join(BASE_PATH, JSON_DIR)

    for filename in DIR_NAMES:
        full_path = os.path.join(jpg_path,filename)
        logging.info(f"Cleaning up: {full_path}.")
        if os.path.exists(full_path):
            logging.info(f"Cleaning up: {full_path}.")
            shutil.rmtree(full_path)
            

# Define DAG
with DAG(
    dag_id="dsv_ocr_workflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual or sensor-based
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    description="Run the OCR workflow.",
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
) as dag:

    environment_preparation = PythonOperator(
        task_id="environment_preparation",
        python_callable=prep_environment
    )
   
    split_pdf_to_jpg = PythonOperator(
        task_id="split_pdf_to_jpg",
        python_callable=convert_pdf_into_jpg_by_page
    )

    convert_jpg_to_markdown = PythonOperator(
        task_id="convert_jpg_to_markdown",
        python_callable=process_img_to_markdown
    )

    combine_pages_with_related_tables = PythonOperator(
        task_id="combine_pages_with_related_tables",
        python_callable=combine_realated_pages
    )

    convert_tables_to_json = PythonOperator(
        task_id="convert_tables_to_json",
        python_callable=convert_merged_tables_to_json
    )

    environment_cleaup = PythonOperator(
        task_id="environment_cleaup",
        python_callable=clean_environment
    )
  
    
    environment_preparation >> split_pdf_to_jpg >> convert_jpg_to_markdown >> combine_pages_with_related_tables >> convert_tables_to_json >> environment_cleaup
