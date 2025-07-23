import os
import sys
sys.path.append('../')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import re
import json
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from logparser import Drain

# === Config ===
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
input_dir = os.path.join(PROJECT_ROOT, 'PROJECT_AI_MODEL', 'datasets', 'Hadoop')
abnormal_file = os.path.join(PROJECT_ROOT, 'PROJECT_AI_MODEL', 'datasets', 'Hadoop', 'abnormal_label.txt')
output_dir = os.path.join(PROJECT_ROOT, 'PROJECT_AI_MODEL', 'datasets', 'Hadoop', 'output')
log_file = "combined_hadoop.log"

log_structured_file = os.path.join(output_dir, log_file + "_structured.csv")
log_templates_file = os.path.join(output_dir, log_file + "_templates.csv")
log_sequence_file = os.path.join(output_dir, "hadoop_sequence.csv")

# === 1. Extract labels ===
def extract_labels_from_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    label_dict = {}
    for label, section in zip([0, 1, 1, 1], content.split("###")[1:]):
        matches = re.findall(r'application_\d+_\d+', section)
        for app_id in matches:
            label_dict[app_id] = label
    return label_dict

# === 2. Merge all logs ===
def merge_logs(input_base_path, output_file):
    with open(output_file, "w") as out:
        for app_folder in os.listdir(input_base_path):
            full_path = os.path.join(input_base_path, app_folder)
            if os.path.isdir(full_path):
                for log_file in os.listdir(full_path):
                    if log_file.endswith(".log"):
                        with open(os.path.join(full_path, log_file), "r") as f:
                            for line in f:
                                line = line.strip()
                                if line:
                                    out.write(f"[{app_folder}] {line}\n")

# === 3. Parse logs using Drain ===
def parse_log_with_drain():
    regex = [
        r"\[application_\d+_\d+\]",
        r"\d{4}-\d{2}-\d{2}",
        r"\d{2}:\d{2}:\d{2},\d{3}",
        r"\b\d+\b",
    ]
    log_format = r'\[<AppId>\] <Date> <Time> <Level> \[<Process>\] <Component>: <Content>'

    parser = Drain.LogParser(
        log_format=log_format,
        indir=input_dir,
        outdir=output_dir,
        depth=5,
        st=0.5,
        rex=regex,
        keep_para=False
    )
    parser.parse(log_file)

    # Confirm parsing
    struct_file = os.path.join(output_dir, log_file + "_structured.csv")
    if os.path.exists(struct_file):
        df = pd.read_csv(struct_file)
        print(f"✅ Parsed lines: {len(df)}")
        print(df.head())
    else:
        print("❌ No structured log file generated.")

# === 4. Map EventIds to integer IDs ===
def mapping():
    df = pd.read_csv(log_templates_file)
    df.sort_values(by="Occurrences", ascending=False, inplace=True)
    template_map = {event: idx + 1 for idx, event in enumerate(df["EventId"])}
    with open(os.path.join(output_dir, "hadoop_log_templates.json"), "w") as f:
        json.dump(template_map, f)

# === 5. Group logs by AppId ===
def hadoop_sampling():
    df = pd.read_csv(log_structured_file)
    with open(os.path.join(output_dir, "hadoop_log_templates.json"), "r") as f:
        event_map = json.load(f)

    df["EventId"] = df["EventId"].apply(lambda x: event_map.get(x, -1))

    data_dict = defaultdict(list)
    for _, row in tqdm(df.iterrows()):
        app_id = row.get("AppId")
        if pd.notnull(app_id):
            data_dict[app_id].append(row["EventId"])

    pd.DataFrame(list(data_dict.items()), columns=['AppId', 'EventSequence']).to_csv(log_sequence_file, index=False)
    print(f"✅ hadoop_sequence.csv written with {len(data_dict)} sequences.")

# === 6. Split into train/test sets ===
def generate_train_test(label_dict, ratio=0.8):
    df = pd.read_csv(log_sequence_file)
    df["Label"] = df["AppId"].apply(lambda x: label_dict.get(x, -1))
    normal_seq = df[df["Label"] == 0]["EventSequence"].sample(frac=1, random_state=42)
    abnormal_seq = df[df["Label"] == 1]["EventSequence"]
    train_len = int(len(normal_seq) * ratio)
    train = normal_seq.iloc[:train_len]
    test_normal = normal_seq.iloc[train_len:]
    test_abnormal = abnormal_seq
    df_to_file(train, os.path.join(output_dir, "train"))
    df_to_file(test_normal, os.path.join(output_dir, "test_normal"))
    df_to_file(test_abnormal, os.path.join(output_dir, "test_abnormal"))

def df_to_file(df, file_name):
    with open(file_name, 'w') as f:
        for _, row in df.items():
            f.write(" ".join([str(e) for e in eval(row)]) + "\n")

# === Main ===
if __name__ == "__main__":
    os.makedirs(output_dir, exist_ok=True)
    label_dict = extract_labels_from_file(abnormal_file)
    merge_logs(input_dir, os.path.join(input_dir, log_file))
    parse_log_with_drain()
    mapping()
    hadoop_sampling()
    generate_train_test(label_dict)