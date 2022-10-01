import psycopg as pg3
from unittest import skip
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


def import_author_table(path_to_author_export, row_range=(0,-1), log_step=10000, clear_table=True, batch_size=128):
    print("...Importing 'authors' data...")
    start_time = time.time()
    prev_block_time = start_time

    create_table_string = """
        CREATE TABLE IF NOT EXISTS authors (
	    id int8 PRIMARY KEY,
        name varchar(255),
        username varchar(255),
        description text,
        followers_count int4,
        following_count int4,
        tweet_count int4,
        listed_count int4
        );
    """

    connection = pg.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                            password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres")
    pg.extensions.register_type(pg.extensions.UNICODE, connection)
    cursor = connection.cursor()

    # create table
    cursor.execute(create_table_string)

    # clear the table if necessary
    if clear_table:
        cursor.execute("""
            DELETE FROM authors;
        """)

    try:
        with gzip.open(path_to_author_export, 'r') as f:
            author_rows_batch = []
        
            for it, author_json_str in enumerate(f):
                if it < row_range[0]:
                    continue
                if row_range[1] != -1 and it >= row_range[1]:
                    break

                author_obj = json.loads(author_json_str)
                author_row = migrate_author_entity(author_obj)

                if author_row is not None:
                    author_rows_batch.append(author_row)
                
                    if len(author_rows_batch) == batch_size:
                        execute_batch(cursor, """
                            INSERT INTO authors (id, name, username, description, 
                            followers_count, following_count, tweet_count, 
                            listed_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            author_rows_batch, page_size=batch_size
                        )

                        author_rows_batch = []

                if it % log_step == 0 and it != 0 and it != row_range[0]:
                    connection.commit()
                    time_check = time.time()

                    elapsed_time = (time_check - start_time) / 60
                    block_time = time_check - prev_block_time

                    pid = os.getpid()

                    print(
                        f"{pid} | it: {it-log_step}-{it} | Time elapsed since the beggining: {elapsed_time:.2f} min | Time spent on the last block: {block_time:.2f}s")
                    prev_block_time = time_check
                
            if len(author_rows_batch) != 0:
                execute_batch(cursor, """
                    INSERT INTO authors (id, name, username, description, 
                    followers_count, following_count, tweet_count, 
                    listed_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    author_rows_batch, page_size=batch_size
                )
                connection.commit()
    except Exception as e:
        print(e)
        print(e.with_traceback())
        raise e

    cursor.close()
    connection.close()
    print("...Finished importing 'authors' data...")


def import_author_table_using_copy(path_to_author_export, row_range=(0,-1), log_step=10000, clear_table=True, batch_size=128):
    print("...Importing 'authors' data...")
    start_time = time.time()
    prev_block_time = start_time

    create_table_string = """
        CREATE TABLE IF NOT EXISTS authors (
	    id int8 PRIMARY KEY,
        name varchar(255),
        username varchar(255),
        description text,
        followers_count int4,
        following_count int4,
        tweet_count int4,
        listed_count int4
        );
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'), 
            password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        
        # pg3.extensions.register_type(pg.extensions.UNICODE, connection)
        with connection.cursor() as cursor:

            # create table
            cursor.execute(create_table_string)

            # clear the table if necessary
            if clear_table:
                cursor.execute("""
                    DELETE FROM authors;
                """)

            with gzip.open(path_to_author_export, 'r') as f:
                author_rows_batch = []
                all_ids = {}
            
                for it, author_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    author_obj = json.loads(author_json_str)
                    author_row = migrate_author_entity(author_obj)

                    if author_row is not None:
                        len_ids = len(all_ids.keys())
                        all_ids[author_row[0]] = "1"
                        new_len_ids = len(all_ids.keys())

                        #if weve got a duplicate id, the size of dictionary remains the same
                        if len_ids != new_len_ids:
                            author_rows_batch.append(author_row)
                        
                            if len(author_rows_batch) == batch_size:
                                with cursor.copy("""
                                    COPY authors (id, name, username, description, 
                                    followers_count, following_count, tweet_count, 
                                    listed_count) FROM STDIN
                                """) as copy:
                                    for author_record in author_rows_batch:
                                        copy.write_row(author_record)

                                author_rows_batch = []

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        connection.commit()
                        time_check = time.time()

                        elapsed_time = (time_check - start_time) / 60
                        block_time = time_check - prev_block_time

                        pid = os.getpid()

                        print(
                            f"{pid} | it: {it-log_step}-{it} | Time elapsed since the beggining: {elapsed_time:.2f} min | Time spent on the last block: {block_time:.2f}s")
                        prev_block_time = time_check
                    
                if len(author_rows_batch) != 0:
                    with cursor.copy("""
                        COPY authors (id, name, username, description, 
                        followers_count, following_count, tweet_count, 
                        listed_count) FROM STDIN
                    """) as copy:
                        for author_record in author_rows_batch:
                            copy.write_row(author_record)
        

    print("...Finished importing 'authors' data...")



def migrate_author_entity(original_obj):
    obj = copy.deepcopy(original_obj)

    if exists(obj, "id", is_id=True) == False:
        return None
    obj["id"] = int(obj["id"])

    nullable_string_attributes = [
        "name",
        "username",
        "description"
    ]
    for str_attrb in nullable_string_attributes:
        if exists(obj, str_attrb) == False:
            obj[str_attrb] = None
        else:
            obj[str_attrb] = make_string_valid(obj[str_attrb])

    public_metrics = [
        "followers_count",
        "following_count"
        "tweet_count"
        "listed_count"
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
                obj["public_metrics"][m] = int(obj["public_metrics"][m])

    table_row = [
        obj["id"],
        obj["name"],
        obj["username"],
        obj["description"],
        obj["public_metrics"]["followers_count"],
        obj["public_metrics"]["following_count"],
        obj["public_metrics"]["tweet_count"],
        obj["public_metrics"]["listed_count"],
    ]

    return table_row

def process_func(row_range):
    path_to_authors = r"C:\Users\marve\authors.jsonl.gz"
    # import_author_table(path_to_authors, row_range=row_range, clear_table=True, log_step=100000, batch_size=1000)
    import_author_table_using_copy(path_to_authors, row_range=row_range, clear_table=True, log_step=100000, batch_size=1000)

    

    print("================ FINISHED PROCESS =================")


if __name__ == "__main__":

    load_dotenv()
    start = time.time()

    process_func([0, -1])

    end = time.time()
    print("COMPLETE TIME SPENT:", end-start)
