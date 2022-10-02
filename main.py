import concurrent.futures
from dotenv import load_dotenv
from functools import partial
import numpy as np

import import_data


def job_dispatcher(all_authors_ids, value):
    path_to_conversations = r"C:\Users\marve\conversations.jsonl.gz"

    row_range = (0, -1)
    # row_range = (0, 300000)

    try:
        if value == "conversations":
            import_data.import_conversation_table(path_to_conversations, all_authors_ids, row_range=row_range, drop_table=False)
        elif value == "context":
            import_data.import_context_domains_entities_annotations_tables(path_to_conversations, row_range=row_range, drop_table=False)
        elif value == "annot_links_refs":
            import_data.import_annotations_links_references_table(path_to_conversations, row_range=row_range, drop_table=False)
        elif value == "hashtags":
            import_data.import_hashtags(path_to_conversations, row_range=row_range, drop_table=False)
        else:
            print(f"ZLY STIRNG: '{value}'")
            raise
    except Exception as e:
        print(e)
        raise


if __name__ == "__main__":
    load_dotenv()
    import_data.drop_all_tables()
    
    path_to_authors = r"C:\Users\marve\authors.jsonl.gz"
    all_author_ids = import_data.import_authors_table(path_to_authors, drop_table=False)
    
    tables_to_import = [    
        "conversations",
        "context",
        "annot_links_refs",
        "hashtags",
    ]

    func = partial(job_dispatcher, all_author_ids)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(func, tables_to_import)

    import_data.remove_references_on_non_existing_conversations()
    import_data.add_table_constraints()
