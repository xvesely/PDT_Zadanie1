import time
import os


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


def exists_same_row(prior_rows, new_row):
    for row in prior_rows:
        eq_num = [r == new_r for r, new_r in zip(row, new_row)]
        if sum(eq_num) == len(row):
            return True

    return False


def log_time(table_name, it, log_step, start_time, prev_block_time):
    time_check = time.time()

    elapsed_time = (time_check - start_time) / 60
    block_time = time_check - prev_block_time

    pid = os.getpid()

    print(
        f"{pid} | {table_name} | it: {it-log_step}-{it} | Since beggining: {elapsed_time:.2f} min | Last block: {block_time:.2f}s")
    
    return time_check


def not_duplicate(all_ids, new_id, cast_to_int=True):
    len_ids = len(all_ids.keys())
    
    if cast_to_int:
        all_ids[int(new_id)] = "1"
    else:
        all_ids[new_id] = "1"

    new_len_ids = len(all_ids.keys())

    return len_ids != new_len_ids


def copy_data_to_table(cursor, query_str, data):
    with cursor.copy(query_str) as copy:
        for record in data:
            copy.write_row(record)

    return []
