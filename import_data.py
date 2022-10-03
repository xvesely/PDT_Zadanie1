from locale import currency
from shutil import which
from turtle import clear
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

import preprocess
from utils import copy_data_to_table, log_time, make_string_valid, not_duplicate


def import_authors_table(path_to_author_export, start_time, row_range=(0,-1), 
                            log_step=1000000, drop_table=True, batch_size=1000):
    print("...Filling 'authors' table...")
    prev_block_time = time.time()

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
    authors_query_string = """
        COPY authors (id, name, username, description, 
        followers_count, following_count, tweet_count, 
        listed_count) FROM STDIN
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'), 
            password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            # create table
            cursor.execute(create_table_string)

            # clear the table if necessary
            if drop_table:
                cursor.execute("""
                    DROP TABLE authors;
                """)

            with gzip.open(path_to_author_export, 'r') as f:
                author_rows_batch = []
                all_author_ids = {}
            
                for it, author_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    author_obj = json.loads(author_json_str)
                    author_row = preprocess.prepare_authors(author_obj)

                    if author_row is not None and not_duplicate(all_author_ids, author_row[0]):
                        author_rows_batch.append(author_row)
                    
                        if len(author_rows_batch) == batch_size:
                            author_rows_batch = copy_data_to_table(
                                cursor, authors_query_string, author_rows_batch)
                            connection.commit()

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        prev_block_time = log_time("authors", it, log_step, start_time, prev_block_time)
                    
                if len(author_rows_batch) != 0:
                    copy_data_to_table(
                        cursor, authors_query_string, author_rows_batch)
                    connection.commit()

    prev_block_time = log_time("authors", it, log_step, start_time, prev_block_time)
    print("...Finished importing 'authors' table...")
    return all_author_ids


def import_conversation_table(path_to_conversation_export, start_time, authors_ids, row_range=(0, -1),
                              log_step=1000000, drop_table=True, batch_size=1000):

    print("...Filling 'conversations' table...")
    prev_block_time = time.time()

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
        created_at TIMESTAMPTZ,
        FOREIGN KEY(author_id) REFERENCES authors (id)
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
                    conversation = preprocess.prepare_conversation(conversation_obj)

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
                            connection.commit()

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        prev_block_time = log_time("conversations", it, log_step, start_time, prev_block_time)

                if len(conversation_rows_batch) != 0:
                    conversation_copy_cmd(
                        cursor, conversation_rows_batch, new_author_rows_to_add)
                    connection.commit()

    prev_block_time = log_time("conversations", it, log_step, start_time, prev_block_time)
    print("...Finished importing 'conversations' table...")

    return all_ids


def conversation_copy_cmd(cursor, conversations, authors):
    if len(authors) > 0:
        with cursor.copy("""
            COPY authors (id, name, username, description, 
            followers_count, following_count, tweet_count, 
            listed_count) FROM STDIN
        """) as copy:
            for author_record in authors:
                copy.write_row(author_record)
    
    with cursor.copy("""
        COPY conversations (id, author_id, content,
        possibly_sensitive, language, source,
        retweet_count, reply_count, like_count,
        quote_count, created_at) FROM STDIN
    """) as copy:
        for conversation_record in conversations:
            copy.write_row(conversation_record)

    return [], []


def import_annotations_links_references_table(path_to_conversation_export, start_time, row_range=(0, -1),
                              log_step=1000000, drop_table=True, batch_size=1000):

    print("...Filling 'annotations' table...")
    print("...Filling 'links' table...")
    print("...Filling 'conversation_references' table...")
    prev_block_time = time.time()

    create_table_string1 = """
        CREATE TABLE IF NOT EXISTS annotations (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        value text NOT NULL,
        type text NOT NULL,
        probability numeric(4,3) NOT NULL,
        FOREIGN KEY(conversation_id) REFERENCES conversations (id)
        );
    """
    create_table_string2 = """
        CREATE TABLE IF NOT EXISTS links (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        url varchar(2048) NOT NULL,
        title text,
        description text,
        FOREIGN KEY(conversation_id) REFERENCES conversations (id)
        );
    """
    create_table_string3 = """
        CREATE TABLE IF NOT EXISTS conversation_references (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        parent_id int8 NOT NULL,
        type varchar(20) NOT NULL,
        FOREIGN KEY(conversation_id) REFERENCES conversations (id),
        FOREIGN KEY(parent_id) REFERENCES conversations (id)
        );
    """

    ####################################

    annotation_copy_query = """
        COPY annotations (conversation_id, value, type, 
        probability) FROM STDIN
    """
    links_copy_query = """
        COPY links (conversation_id, url, title, 
        description) FROM STDIN
    """
    references_copy_query = """
        COPY conversation_references (conversation_id, 
        parent_id, type) FROM STDIN
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                     password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            if drop_table:
                cursor.execute("""
                    DROP TABLE IF EXISTS annotations;
                """)
                cursor.execute("""
                    DROP TABLE IF EXISTS links;
                """)
                cursor.execute("""
                    DROP TABLE IF EXISTS conversation_references;
                """)
                
            cursor.execute(create_table_string1)
            cursor.execute(create_table_string2)
            cursor.execute(create_table_string3)

            with gzip.open(path_to_conversation_export, 'r') as f:
                annotation_rows_batch = []
                link_rows_batch = []
                references_rows_batch = []

                conversation_ids = {}

                cursor.execute("""
                        SELECT id FROM conversations
                    """)
                all_possible_parent_id_values = cursor.fetchall()
                all_possible_parent_id_values = {item[0]:"1" for item in all_possible_parent_id_values}
            
                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    conversation_obj = json.loads(conversation_json_str)
                    
                    if preprocess.check_conversation_validity(conversation_obj) and not_duplicate(conversation_ids, conversation_obj["id"]):
                        annotation_arr = preprocess.prepare_annotations(conversation_obj)
                        links_arr = preprocess.prepare_links(conversation_obj)
                        references_arr = preprocess.prepare_conversation_references(conversation_obj)
                        
                        to_commit = False

                        if annotation_arr is not None:
                            annotation_rows_batch.extend(annotation_arr)
                            if len(annotation_rows_batch) >= batch_size:
                                annotation_rows_batch = copy_data_to_table(
                                    cursor, annotation_copy_query, annotation_rows_batch)
                                to_commit = True
                                
                        if links_arr is not None:
                            link_rows_batch.extend(links_arr)
                            if len(link_rows_batch) >= batch_size:
                                link_rows_batch = copy_data_to_table(
                                    cursor, links_copy_query, link_rows_batch)
                                to_commit = True
                            
                        if references_arr is not None:
                            valid_references = list(filter(lambda ref: ref[1] in all_possible_parent_id_values, references_arr))

                            references_rows_batch.extend(valid_references)
                            if len(references_rows_batch) >= batch_size:
                                references_rows_batch = copy_data_to_table(
                                    cursor, references_copy_query, references_rows_batch)
                                to_commit = True
                                
                        if to_commit:
                            connection.commit()

                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        prev_block_time = log_time("annot-links-refs", it, log_step, start_time, prev_block_time)

                if len(annotation_rows_batch) != 0:
                    copy_data_to_table(cursor, annotation_copy_query, annotation_rows_batch)
                    connection.commit()

                if len(link_rows_batch) != 0:
                    copy_data_to_table(cursor, links_copy_query, link_rows_batch)
                    connection.commit()

                if len(references_rows_batch) != 0:
                    copy_data_to_table(cursor, references_copy_query, references_rows_batch)
                    connection.commit()

    prev_block_time = log_time("annot-links-refs", it, log_step, start_time, prev_block_time)
    print("...Finished importing 'annotations' table...")
    print("...Finished importing 'links' table...")
    print("...Finished importing 'conversation_references' table...")

def import_context_domains_entities_annotations_tables(path_to_conversation_export, start_time, row_range=(0, -1),
                            log_step=1000000, drop_table=True, batch_size=1000):
    
    print("...Filling 'context_domains' table...")
    print("...Filling 'context_entities' table...")
    print("...Filling 'context_annotation' table...")
    prev_block_time = time.time()

    create_table_string1 = """
        CREATE TABLE IF NOT EXISTS context_domains (
        id int8 PRIMARY KEY,
        name varchar(255) NOT NULL,
        description text
        );
    """
    create_table_string2 = """
        CREATE TABLE IF NOT EXISTS context_entities (
        id int8 PRIMARY KEY,
        name varchar(255) NOT NULL,
        description text
        );
    """
    create_table_string3 = """
        CREATE TABLE IF NOT EXISTS context_annotations (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        context_domain_id int8 NOT NULL,
        context_entity_id int8 NOT NULL,
        FOREIGN KEY(conversation_id) REFERENCES conversations (id),
        FOREIGN KEY(context_domain_id) REFERENCES context_domains (id),
        FOREIGN KEY(context_entity_id) REFERENCES context_entities (id)
        );
    """

    ######################

    
    domain_query_string = """
        COPY context_domains (id, name,
        description) FROM STDIN
    """
    entity_query_string = """
        COPY context_entities (id, name,
        description) FROM STDIN
    """
    annotation_query_string = """
        COPY context_annotations (conversation_id, 
        context_domain_id, context_entity_id) 
        FROM STDIN
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                     password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            if drop_table:
                cursor.execute("""
                    DROP TABLE IF EXISTS context_domains;
                """)
                cursor.execute("""
                    DROP TABLE IF EXISTS context_entities;
                """)
                cursor.execute("""
                    DROP TABLE IF EXISTS context_annotations;
                """)
                
            cursor.execute(create_table_string1)
            cursor.execute(create_table_string2)
            cursor.execute(create_table_string3)

            with gzip.open(path_to_conversation_export, 'r') as f:
                domain_rows_batch = []
                entity_rows_batch = []
                annotation_rows_batch = []

                conversation_ids = {}
                domain_ids = {}
                entity_ids = {}
                
                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    conversation_obj = json.loads(conversation_json_str)
                    
                    if preprocess.check_conversation_validity(conversation_obj) and not_duplicate(conversation_ids, conversation_obj["id"]):
                        domain_arr, entity_arr, annotation_arr = preprocess.prepare_context_annotations(conversation_obj)

                        if domain_arr is not None:
                            new_domains = []
                            new_domains = list(filter(lambda d: not_duplicate(domain_ids, d[0]), domain_arr))

                            new_entities = []
                            new_entities = list(filter(lambda e: not_duplicate(entity_ids, e[0]), entity_arr))

                            domain_rows_batch.extend(new_domains)
                            entity_rows_batch.extend(new_entities)
                            annotation_rows_batch.extend(annotation_arr)

                            if it % batch_size == 0:
                                domain_rows_batch = copy_data_to_table(
                                    cursor, domain_query_string, domain_rows_batch)

                                entity_rows_batch = copy_data_to_table(
                                    cursor, entity_query_string, entity_rows_batch)
                                
                                annotation_rows_batch = copy_data_to_table(
                                    cursor, annotation_query_string, annotation_rows_batch)
                                
                                connection.commit()
                    
                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        prev_block_time = log_time("context", it, log_step, start_time, prev_block_time)

                if len(domain_rows_batch) + len(entity_rows_batch) + len(annotation_rows_batch) > 0:
                    copy_data_to_table(cursor, domain_query_string, domain_rows_batch)
                    copy_data_to_table(cursor, entity_query_string, entity_rows_batch)
                    copy_data_to_table(cursor, annotation_query_string, annotation_rows_batch)

                    connection.commit()

    prev_block_time = log_time("context", it, log_step, start_time, prev_block_time)
    print("...Finished importing 'context_domains' table...")
    print("...Finished importing 'context_entities' table...")
    print("...Finished importing 'context_annotation' table...")


def import_hashtags(path_to_conversation_export, start_time, row_range=(0, -1),
                            log_step=1000000, drop_table=True, batch_size=1000):
    
    print("...Filling 'hashtags' table...")
    print("...Filling 'conversation_hashtags' table...")
    prev_block_time = time.time()

    create_table_string1 = """
        CREATE TABLE IF NOT EXISTS hashtags (
        id int8 PRIMARY KEY,
        tag text UNIQUE NOT NULL
        );
    """
    create_table_string2 = """
        CREATE TABLE IF NOT EXISTS conversation_hashtags (
        id BIGSERIAL PRIMARY KEY,
        conversation_id int8 NOT NULL,
        hashtag_id int8 NOT NULL,
        FOREIGN KEY(conversation_id) REFERENCES conversations (id),
        FOREIGN KEY(hashtag_id) REFERENCES hashtags (id)
        );
    """

    ######################

    
    hashtag_query_string = """
        COPY hashtags (id, tag) FROM STDIN
    """
    conv_hash_query_string = """
        COPY conversation_hashtags 
        (conversation_id, hashtag_id) 
        FROM STDIN
    """

    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                     password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            if drop_table:
                cursor.execute("""
                    DROP TABLE IF EXISTS hashtags CASCADE;
                """)
                cursor.execute("""
                    DROP TABLE IF EXISTS conversation_hashtags;
                """)
                
            cursor.execute(create_table_string1)
            cursor.execute(create_table_string2)
            
            with gzip.open(path_to_conversation_export, 'r') as f:
                hashtag_rows_batch = []
                conv_hash_rows_batch = []

                conversation_ids = {}
                
                new_tag_dict = {}
                dict_tag_to_id = {}
                serial_number = 1
                
                for it, conversation_json_str in enumerate(f):
                    if it < row_range[0]:
                        continue
                    if row_range[1] != -1 and it >= row_range[1]:
                        break

                    conversation_obj = json.loads(conversation_json_str)
                    
                    if preprocess.check_conversation_validity(conversation_obj) and not_duplicate(conversation_ids, conversation_obj["id"]):
                        hashtag_arr = preprocess.prepare_hashtags(conversation_obj)

                        new_hashtags = []
                        new_conv_hash = []

                        if hashtag_arr is not None:
                            for tag in hashtag_arr:
                                if not_duplicate(new_tag_dict, tag[0], cast_to_int=False):
                                    dict_tag_to_id[tag[0]] = serial_number
                                    new_hashtags.append([serial_number, tag[0]])
                                    
                                    serial_number += 1
                                new_conv_hash.append([
                                    int(conversation_obj["id"]),
                                    dict_tag_to_id[tag[0]]
                                ])
                                
                            hashtag_rows_batch.extend(new_hashtags)
                            conv_hash_rows_batch.extend(new_conv_hash)

                            if it % batch_size == 0:
                                hashtag_rows_batch = copy_data_to_table(
                                    cursor, hashtag_query_string, hashtag_rows_batch)
                                conv_hash_rows_batch = copy_data_to_table(
                                    cursor, conv_hash_query_string, conv_hash_rows_batch)
                                
                                connection.commit()
                    
                    if it % log_step == 0 and it != 0 and it != row_range[0]:
                        prev_block_time = log_time("hashtags", it, log_step, start_time, prev_block_time)

                if len(hashtag_rows_batch) + len(conv_hash_rows_batch) > 0:
                    copy_data_to_table(cursor, hashtag_query_string, hashtag_rows_batch)
                    copy_data_to_table(cursor, conv_hash_query_string, conv_hash_rows_batch)

                    connection.commit()

    prev_block_time = log_time("hashtags", it, log_step, start_time, prev_block_time)
    print("...Finish importing 'hashtags' table...")
    print("...Finish importing 'conversation_hashtags' table...")

def drop_all_tables():    
    with pg3.connect(host="localhost", user=os.getenv('PDT_POSTGRES_USER'),
                        password=os.getenv('PDT_POSTGRES_PASS'), dbname="postgres") as connection:

        with connection.cursor() as cursor:

            cursor.execute("DROP TABLE IF EXISTS authors CASCADE")
            cursor.execute("DROP TABLE IF EXISTS conversations CASCADE")
            cursor.execute("DROP TABLE IF EXISTS hashtags CASCADE")
            cursor.execute("DROP TABLE IF EXISTS conversation_hashtags CASCADE")
            cursor.execute("DROP TABLE IF EXISTS conversation_references CASCADE")
            cursor.execute("DROP TABLE IF EXISTS links CASCADE")
            cursor.execute("DROP TABLE IF EXISTS annotations CASCADE")
            cursor.execute("DROP TABLE IF EXISTS context_domains CASCADE")
            cursor.execute("DROP TABLE IF EXISTS context_entities CASCADE")
            cursor.execute("DROP TABLE IF EXISTS context_annotations CASCADE")

            connection.commit()
