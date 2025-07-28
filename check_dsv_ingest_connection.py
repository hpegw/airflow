from airflow.hooks.base import BaseHook
import os
import logging

conn = BaseHook.get_connection("dsv_ingest")
path = conn.extra_dejson.get("path")

logging.info(f""Path from connection:: {path}")
