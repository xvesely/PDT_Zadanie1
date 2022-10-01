from __future__ import barry_as_FLUFL
from importlib.resources import path
import psycopg as pg3
import psycopg2 as pg
import psycopg2.extensions
from psycopg2.extras import execute_batch
import copy
import time
import gzip
import json
import os
from multiprocessing import Pool
import concurrent.futures
from dotenv import load_dotenv


def exists(obj, attr, is_id=False):
    if attr in obj.keys() and obj[attr] is not None:
        if is_id and obj[attr] == "":
            return False
        return True
    return False


def make_string_valid(string):
    string = (str(string)
              .encode("utf-8")
              .decode("utf-8", errors="replace")
              .replace("\x00", "\uFFFD")
              )
    return string


def import_conversation_table(path_to_conversation_export, authors_ids, row_range=(0, -1),
                              log_step=100000, drop_table=True, batch_size=1000):

    print("...Filling 'conversations' table...")
    start_time = time.time()
    prev_block_time = start_time

    create_table_string = """
        CREATE TABLE IF NOT EXISTS conversations (
        id int8 PRIMARY KEY,
        author_id int8 NOT NULL,
        content text NOT NULL,
        possibly_sensitive bool NOT NULL,
        language varchar(3) NOT NULL,
        source text NOT NULL,
        retweet_count int4,
        reply_count int4,
        like_count int4,
        quote_count int4,
        created_at TIMESTAMPTZ
        );
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                     password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            # drop the table if necessary
            if drop_table:
                cursor.execute("""
                    DROP TABLE IF EXISTS conversations;
                """)
                cursor.execute("""
                    DELETE FROM authors; 
                """)
                #TODO teraz tam mam aj authorov, lebo som ich nechceme vymazal...

            # create table
            cursor.execute(create_table_string)

            with gzip.open(path_to_conversation_export, 'r') as f:
                conversation_rows_batch = []
                new_author_rows_to_add = []

                all_ids = {}

                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    conversation_obj = json.loads(conversation_json_str)
                    conversation = prepare_conversation(conversation_obj)

                    # if weve got a duplicate id, the size of dictionary remains the same
                    if conversation is not None and not_duplicate(all_ids, conversation[0]):
                        conversation_rows_batch.append(conversation)

                        if not_duplicate(authors_ids, conversation[1]):
                            new_author_rows_to_add.append(
                                [conversation[1]] + [None]*7
                            )

                        if len(conversation_rows_batch) == batch_size:
                            conversation_rows_batch, new_author_rows_to_add = conversation_copy_cmd(
                                cursor, conversation_rows_batch, new_author_rows_to_add)

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        connection.commit()
                        prev_block_time = log_time(it, log_step, start_time, prev_block_time)

                if len(conversation_rows_batch) != 0:
                    conversation_rows_batch, new_author_rows_to_add = conversation_copy_cmd(
                                    cursor, conversation_rows_batch, new_author_rows_to_add)
                    connection.commit()


def conversation_copy_cmd(cursor, conversations, authors):
    with cursor.copy("""
        COPY conversations (id, author_id, content,
        possibly_sensitive, language, source,
        retweet_count, reply_count, like_count,
        quote_count, created_at) FROM STDIN
    """) as copy:
        for conversation_record in conversations:
            copy.write_row(conversation_record)

    if len(authors) > 0:
        with cursor.copy("""
            COPY authors (id, name, username, description, 
            followers_count, following_count, tweet_count, 
            listed_count) FROM STDIN
        """) as copy:
            for author_record in authors:
                copy.write_row(author_record)

    return [], []


def log_time(it, log_step, start_time, prev_block_time):
    time_check = time.time()

    elapsed_time = (time_check - start_time) / 60
    block_time = time_check - prev_block_time

    pid = os.getpid()

    print(
        f"{pid} | it: {it-log_step}-{it} | Time elapsed since the beggining: {elapsed_time:.2f} min | Time spent on the last block: {block_time:.2f}s")
    
    return time_check


def not_duplicate(all_ids, new_id):
    len_ids = len(all_ids.keys())
    all_ids[int(new_id)] = "1"
    new_len_ids = len(all_ids.keys())

    return len_ids != new_len_ids


def import_annotation_table(path_to_conversation_export, row_range=(0, -1),
                              log_step=100000, drop_table=True, batch_size=1000):

    print("...Filling 'annotations' table...")
    start_time = time.time()
    prev_block_time = start_time

    create_table_string = """
        CREATE TABLE IF NOT EXISTS annotations (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        value text NOT NULL,
        type text NOT NULL,
        probability numeric(4,3) NOT NULL
        );
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                     password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            if drop_table:
                cursor.execute("""
                    DROP TABLE IF EXISTS annotations;
                """)
                
            cursor.execute(create_table_string)

            with gzip.open(path_to_conversation_export, 'r') as f:
                annotation_rows_batch = []
                conversation_ids = {}
                
                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    conversation_obj = json.loads(conversation_json_str)
                    
                    if check_conversation_validity(conversation_obj) and not_duplicate(conversation_ids, conversation_obj["id"]):
                        annotation_arr = prepare_annotations(conversation_obj)

                        if annotation_arr is not None:
                            annotation_rows_batch.extend(annotation_arr)
                            
                            if len(annotation_rows_batch) >= batch_size:
                                with cursor.copy("""
                                    COPY annotations (conversation_id, value, type, 
                                    probability) FROM STDIN
                                """) as copy:
                                    for annotation_record in annotation_rows_batch:
                                        copy.write_row(annotation_record)

                                annotation_rows_batch = []

                        if it % log_step == 0 and it != 0 and it != row_range[0]:
                            connection.commit()
                            prev_block_time = log_time(it, log_step, start_time, prev_block_time)

                if len(annotation_rows_batch) != 0:
                    with cursor.copy("""
                        COPY annotations (conversation_id, value, type, 
                        probability) FROM STDIN
                    """) as copy:
                        for annotation_record in annotation_rows_batch:
                            copy.write_row(annotation_record)
                    connection.commit()



# TODO - not finished yet
def import_hashtags(path_to_conversation_export, row_range=(0, -1),
                            log_step=10000, drop_table=True, batch_size=1000):
    
    print("...Filling 'hashtags' and 'conversation_hashtags' tables...")
    start_time = time.time()
    prev_block_time = start_time

    create_table_string1 = """
        CREATE TABLE IF NOT EXISTS hashtags (
        id int8 PRIMARY KEY,
        tag text NOT NULL
        );
    """
    create_table_string2 = """
        CREATE TABLE IF NOT EXISTS conversation_hashtags (
        id int8 PRIMARY KEY,
        conversation_id int8 NOT NULL,
        hashtag_id int8 NOT NULL
        );
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                    password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            if drop_table:
                cursor.execute("""
                    DELETE FROM hashtags;
                """)
                cursor.execute("""
                    DELETE FROM conversations_hashtags;
                """)

            cursor.execute(create_table_string1)
            cursor.execute(create_table_string2)

            with gzip.open(path_to_conversation_export, 'r') as f:
                conversation_rows_batch = []
                all_tags = {}

                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break
                    conversation_obj = json.loads(conversation_json_str)

                    if check_conversation_validity(conversation_obj):
                        hashtag_arr = prepare_hashtags(conversation_obj)

                        if hashtag_arr is not None:
                            indices = get_indices_of_new_unique_tags(hashtag_arr, all_tags)

                            len_ids = len(all_tags.keys())
                            all_tags[conversation[0]] = "1"
                            new_len_ids = len(all_tags.keys())

                            # if weve got a duplicate id, the size of dictionary remains the same
                            if len_ids != new_len_ids:
                                conversation_rows_batch.append(conversation)

                                len_ids = len(authors_ids.keys())
                                authors_ids[conversation[1]] = "1"
                                new_len_ids = len(authors_ids.keys())

                                # add a new author to table if not exists
                                if len_ids != new_len_ids:
                                    new_author_rows_to_add.append(
                                        [conversation[1]] + [None]*7
                                    )

                                if len(conversation_rows_batch) == batch_size:
                                    conversation_rows_batch, new_author_rows_to_add = conversation_copy_cmd(
                                        cursor, conversation_rows_batch, new_author_rows_to_add)

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        connection.commit()
                        prev_block_time = log_time(it, log_step, start_time, prev_block_time)

                if len(conversation_rows_batch) != 0:
                    conversation_rows_batch, new_author_rows_to_add = conversation_copy_cmd(
                                    cursor, conversation_rows_batch, new_author_rows_to_add)


def get_indices_of_new_unique_tags(hashtag_arr, all_tags):
    
    
    pass


def check_conversation_validity(obj):
    attrs_to_check = [
        "id",
        "author_id",
        "created_at"
    ]
    for attr in attrs_to_check:
        if exists(obj, attr, is_id=True) == False:
            return False
        if attr == "id" or attr == "author_id":
            try:
                x = int(obj[attr])
            except:
                return False

    return True
    

def prepare_conversation(original_obj, prepare_other_models=False):
    obj = copy.deepcopy(original_obj)

    if exists(obj, "id", is_id=True) == False:
        return None
    try:
        obj["id"] = int(obj["id"])
    except:
        return None

    if exists(obj, "author_id", is_id=True) == False:
        return None
    try:
        obj["author_id"] = int(obj["author_id"])
    except:
        return None

    possibly_string_attributes = [
        "text",
        "lang",
        "source"
    ]

    for attr in possibly_string_attributes:
        if exists(obj, attr) == False:
            obj[attr] = ""
        obj[attr] = make_string_valid(obj[attr])

    obj["lang"] = obj["lang"][:3]

    try:
        obj["possibly_sensitive"] = bool(obj["possibly_sensitive"])
    except:
        obj["possibly_sensitive"] = False

    public_metrics = [
        "retweet_count",
        "reply_count",
        "like_count",
        "quote_count",
    ]

    if exists(obj, "public_metrics") == False:
        obj["public_metrics"] = {}

        for m in public_metrics:
            obj["public_metrics"][m] = None
    else:
        for m in public_metrics:
            if exists(obj["public_metrics"], m) == False:
                obj["public_metrics"][m] = None
            else:
                try:
                    obj["public_metrics"][m] = int(obj["public_metrics"][m])
                except:
                    obj["public_metrics"][m] = None

    if exists(obj, "created_at") == False:
        return None
    obj["created_at"] = make_string_valid(obj["created_at"])

    conversation_row = [
        obj["id"],
        obj["author_id"],
        obj["text"],
        obj["possibly_sensitive"],
        obj["lang"],
        obj["source"],
        obj["public_metrics"]["retweet_count"],
        obj["public_metrics"]["reply_count"],
        obj["public_metrics"]["like_count"],
        obj["public_metrics"]["quote_count"],
        obj["created_at"]
    ]

    if prepare_other_models == False:
        return conversation_row

    hashtag_rows = prepare_hashtags(obj)
    annotation_rows = prepare_annotations(obj)
    link_rows = prepare_links(obj)
    context_domain_rows, context_entity_rows, context_annotation_rows = prepare_context_annotations(obj)
    conversation_reference_rows = prepare_conversation_references(obj)

    return (
        conversation_row,
        hashtag_rows,
        annotation_rows,
        link_rows,
        context_domain_rows,
        context_entity_rows,
        context_annotation_rows,
        conversation_reference_rows
    )


def exists_same_row(prior_rows, new_row):
    for row in prior_rows:
        eq_num = [r == new_r for r, new_r in zip(row, new_row)]
        if sum(eq_num) == len(row):
            return True

    return False


def prepare_hashtags(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "hashtags"):
        hashtags_arr = []

        for hashtag in entities["hashtags"]:
            if exists(hashtag, "tag") and len(hashtag["tag"]) > 0:
                hashtags_arr.append(make_string_valid(hashtag["tag"]))

        hashtags_arr = list(set(hashtags_arr))
        return [[tag] for tag in hashtags_arr]
    return None


def prepare_annotations(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "annotations"):
        annotations_arr = []

        for annot in entities["annotations"]:
            attr_names = [
                "normalized_text",
                "type",
                "probability"
            ]
            attr_states = [exists(annot, attr) for attr in attr_names]
            if sum(attr_states) != len(attr_names):
                continue

            values_to_add = ([conversation["id"]] +
                             [make_string_valid(annot[attr]) for attr in attr_names])

            if exists_same_row(annotations_arr, values_to_add) == False:
                annotations_arr.append(values_to_add)

        return annotations_arr
    return None


def prepare_links(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "urls"):
        link_arr = []

        for link in entities["urls"]:
            attr_names = [
                "expanded_url",
                "title",
                "description"
            ]
            values = [conversation["id"]]

            if exists(link, "expanded_url") == False:
                continue

            for attr in attr_names:
                if exists(link, attr) == False:
                    values.append(None)
                else:
                    values.append(make_string_valid(link[attr]))

            values[1] = values[1][:2048]

            if exists_same_row(link_arr, values) == False:
                link_arr.append(values)

        return link_arr
    return None


def prepare_context_annotations(conversation):
    if exists(conversation, "context_annotations") == False:
        return None, None, None

    context_annotations = conversation["context_annotations"]
    domain_arr = []
    entity_arr = []
    domain_entity_rels = []

    for context in context_annotations:
        main_attr = [
            "domain",
            "entity"
        ]
        attr_states = [exists(context, attr) for attr in main_attr]

        if sum(attr_states) != len(main_attr):
            continue

        two_objects = []
        for attr in main_attr:
            obj = copy.deepcopy(context[attr])

            if exists(obj, "id", is_id=True) == False or exists(obj, "name") == False:
                break
            try:
                obj["id"] = int(obj["id"])
            except:
                break
    
            obj["name"] = make_string_valid(obj["name"])

            if exists(obj, "description"):
                obj["description"] = make_string_valid(obj["description"])
            else:
                obj["description"] = None

            two_objects.append([
                obj["id"],
                obj["name"][:255],
                obj["description"]
            ])

        if len(two_objects) != 2:
            continue

        domain, entity = two_objects

        if exists_same_row(domain_arr, domain) == False:
            domain_arr.append(domain)
        if exists_same_row(entity_arr, entity) == False:
            entity_arr.append(entity)

        new_domain_entity_rel = [
            conversation["id"],
            domain[0],
            entity[0]
        ]
        if exists_same_row(domain_entity_rels, new_domain_entity_rel) == False:
            domain_entity_rels.append(new_domain_entity_rel)

    if len(domain_arr) > 0:
        return domain_arr, entity_arr, domain_entity_rels
    return None, None, None


def prepare_conversation_references(conversation):
    if exists(conversation, "referenced_tweets") == False:
        return None

    references_arr = []
    references = conversation["referenced_tweets"]

    for ref in references:
        if exists(ref, "id", is_id=True) == False or exists(ref, "type") == False:
            continue

        values = []
        try:
            values.append(int(ref["id"]))
        except:
            continue

        values.append(conversation["id"])
        values.append(make_string_valid(ref["type"])[:20])

        if exists_same_row(references_arr, values) == False:
            references_arr.append(values)

    if len(references_arr) > 0:
        return references_arr
    return None

def job_dispatcher(value):
    path_to_conversations = r"C:\Users\marve\conversations.jsonl.gz"

    if value == 0:
        with open("author_ids.json", "r") as f:
            author_ids = json.load(f)

        author_dict_ids = {
            item: "1" for item in author_ids
        }

        import_conversation_table(path_to_conversations, author_dict_ids)
    else:
        import_annotation_table(path_to_conversations)

if __name__ == "__main__":
    path_to_conversations = r"C:\Users\marve\conversations.jsonl.gz"

    # with open("author_ids.json", "r") as f:
    #     author_ids = json.load(f)

    # author_dict_ids = {
    #     item: "1" for item in author_ids
    # }

    # import_conversation_table(path_to_conversations, author_dict_ids)

    tables = [0, 1]

    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        executor.map(job_dispatcher, tables)

    # import_conversation_table(path_to_conversations, author_dict_ids)
    # import_annotation_table(path_to_conversations)
