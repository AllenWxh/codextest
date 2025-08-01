import json
import pandas as pd
from datetime import datetime
import argparse

def merge_and_deduplicate(
    hot_file,
    potential_file,
    user_score_file,
    output_file
):
    # 加载用户影响力分数
    user_scores = pd.read_csv(user_score_file)
    user_score_dict = dict(zip(user_scores["user_name"], user_scores["influence_score"]))

    # 处理 related_posts.json
    with open(hot_file, "r", encoding="utf-8") as f:
        hot_data = [json.loads(line) for line in f if line.strip()]

    hot_records = []
    for post in hot_data:
        hours_ago = (datetime.utcnow() - datetime.utcfromtimestamp(post["created_utc"])).total_seconds() / 3600
        hot_records.append({
            "title": post["title"],
            "author": post["author_name"],
            "url": post["url"],
            "score": post["score"],
            "time": f"{round(hours_ago, 2)} hours before",
            "author_influence": user_score_dict.get(post["author_name"], 0)
        })

    # 处理 related_posts_2.json
    with open(potential_file, "r", encoding="utf-8") as f:
        potential_data = [json.loads(line) for line in f if line.strip()]

    potential_records = []
    for post in potential_data:
        potential_records.append({
            "title": post["post_title"],
            "author": post["user_name"],
            "url": post["url"],
            "score": post["current_score"],
            "time": f"{round(post["time_since_post_hours"], 2)} hours before",
            "author_influence": user_score_dict.get(post["user_name"], 0)
        })

    # 合并并去重（按标题去重，保留第一次出现）
    all_records = hot_records + potential_records
    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset="title", keep="first")

    # 导出结果
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"合并完成，共 {len(df)} 条帖子进入贴池")

def main():
    parser = argparse.ArgumentParser(description="合并并去重两个来源的相关帖子，输出用户影响力结果")
    parser.add_argument("--hot_file", type=str, default="related_posts.json", help="热点帖子文件路径")
    parser.add_argument("--potential_file", type=str, default="related_posts_2.json", help="潜在帖子文件路径")
    parser.add_argument("--user_score_file", type=str, default="user_influence_scores.csv", help="用户影响力得分文件")
    parser.add_argument("--output_file", type=str, default="merged_posts_with_scores.csv", help="合并输出文件路径")

    args = parser.parse_args()

    merge_and_deduplicate(
        hot_file=args.hot_file,
        potential_file=args.potential_file,
        user_score_file=args.user_score_file,
        output_file=args.output_file
    )

if __name__ == "__main__":
    main()
