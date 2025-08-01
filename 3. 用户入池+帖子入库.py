import pandas as pd
import json
import os
import argparse
import sys

def load_cache(filepath):
    try:
        df = pd.read_json(filepath, lines=True)
        if df.empty or not all(col in df.columns for col in ["author_name", "author_id", "id"]):
            print("警告：输入文件为空或缺少必要字段，跳过本模块")
            return None
        return df
    except Exception as e:
        print(f"读取文件失败：{e}")
        return None

def extract_users(df, filepath):
    df_new = df[["author_name", "author_id"]].drop_duplicates()

    if os.path.exists(filepath):
        df_old = pd.read_json(filepath, lines=True)
        df_merged = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()
    else:
        df_merged = df_new

    df_merged.to_json(filepath, orient="records", lines=True, force_ascii=False)

def extract_training_posts(df, filepath):
    df_new = df.copy()
    df_new["label"] = "行业相关"

    if os.path.exists(filepath):
        df_old = pd.read_json(filepath, lines=True)
        df_merged = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["id"])
    else:
        df_merged = df_new

    df_merged.to_json(filepath, orient="records", lines=True, force_ascii=False)

def main(related_posts_file, users_output_file, posts_output_file):
    df = load_cache(related_posts_file)
    if df is None:
        return

    extract_users(df, users_output_file)
    extract_training_posts(df, posts_output_file)

    print("缓存数据已分拆入库")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="related_posts.json", help="输入文件路径")
    parser.add_argument("--users_output", type=str, default="users_raw_pool.json", help="用户输出路径")
    parser.add_argument("--posts_output", type=str, default="posts_raw_pool.json", help="帖子输出路径")
    args = parser.parse_args()

    main(
        related_posts_file=args.input,
        users_output_file=args.users_output,
        posts_output_file=args.posts_output
    )
