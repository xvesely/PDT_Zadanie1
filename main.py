import concurrent.futures
import gzip
import json
from dotenv import load_dotenv
from functools import partial
import time 
import numpy as np
import os

import import_data
from preprocess import prepare_conversation


def job_dispatcher(start_time, value):
    path_to_conversations = r"C:\Users\marve\conversations.jsonl.gz"

    row_range = (0, -1)

    try:
        if value == "context":
            import_data.import_context_domains_entities_annotations_tables(
                path_to_conversations, start_time, row_range=row_range, drop_table=False, log_step=1000000)
        elif value == "annot_links_refs":
            import_data.import_annotations_links_references_table(
                path_to_conversations, start_time, row_range=row_range, drop_table=False, log_step=1000000)
        elif value == "hashtags":
            import_data.import_hashtags(
                path_to_conversations, start_time, row_range=row_range, drop_table=False, log_step=1000000)
        else:
            print(f"ZLY STIRNG: '{value}'")
            raise
    except Exception as e:
        print(e)
        raise


if __name__ == "__main__":
    START_TIME = time.time()
    
    # remove logs file from previous run
    if os.path.exists("./logs"):
        for file in os.listdir("./logs"):
            fullpath = os.path.join("./logs", file)
            os.remove(fullpath)

    load_dotenv()

    import_data.drop_all_tables()
    
    path_to_authors = r"C:\Users\marve\authors.jsonl.gz"
    path_to_conversations = r"C:\Users\marve\conversations.jsonl.gz"

    all_author_ids = import_data.import_authors_table(path_to_authors, START_TIME, drop_table=False, log_step=1000000)
    import_data.import_conversation_table(path_to_conversations, START_TIME, all_author_ids, drop_table=False, log_step=1000000)

    tables_to_import = [
        "context",
        "annot_links_refs",
        "hashtags",
    ]

    func = partial(job_dispatcher, START_TIME)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(func, tables_to_import)
