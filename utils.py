import time
import os
import pandas as pd
from datetime import datetime


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


def log_time(table_name, it, log_step, start_time, prev_block_time, log_to_console=True):
    current_time = datetime.now().isoformat()
    t_idx = current_time.find("T")
    current_time = current_time[:t_idx+6] + "Z"
    
    time_checkpoint = time.time()
    elapsed_time = time_checkpoint - start_time
    block_time = time_checkpoint - prev_block_time  

    elapsed_time = format_duration(elapsed_time)
    block_time = format_duration(block_time)

    pid = os.getpid()

    if log_to_console:
        print(
            f"{pid} | {table_name} | it: {it-log_step}-{it} | Since beggining: {elapsed_time} | Last block: {block_time}")
    
    os.makedirs("./logs", exist_ok=True)

    try:
        df = pd.read_csv(f"./logs/{table_name}.csv", sep=";", header=None)
    except:
        df = pd.DataFrame(columns=["date", "all-time", "block-time"])

    df.loc[len(df.index)] = [
        current_time,
        elapsed_time,
        block_time
    ]
    df.to_csv(f"./logs/{table_name}.csv", sep=";", index=False, header=False)

    return time_checkpoint


def format_duration(duration):
    mins = int(duration / 60)
    secs = int(duration) % 60

    return f"{mins:02d}:{secs:02d}"

def not_duplicate(all_ids, new_id, cast_to_int=True):
    len_ids = len(all_ids.keys())
    
    if cast_to_int:
        all_ids[int(new_id)] = "1"
    else:
        all_ids[new_id] = "1"

    new_len_ids = len(all_ids.keys())

    return len_ids != new_len_ids


def copy_data_to_table(cursor, query_str, data):
    if len(data) == 0:
        return []
    
    with cursor.copy(query_str) as copy:
        for record in data:
            copy.write_row(record)

    return []
