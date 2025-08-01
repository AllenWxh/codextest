import json
import pandas as pd
from datetime import datetime
import argparse

def normalize(series):
    return (series - series.min()) / (series.max() - series.min() + 1e-9)

def compute_influence_scores(static_path, dynamic_path, output_path="user_influence_scores.csv"):
    with open(static_path, "r", encoding="utf-8") as f:
        static_data = [json.loads(line) for line in f]
    df_static = pd.DataFrame(static_data)

    with open(dynamic_path, "r", encoding="utf-8") as f:
        dynamic_data = [json.loads(line) for line in f]
    df_dynamic = pd.DataFrame(dynamic_data)

    df = pd.merge(df_dynamic, df_static[["user_id", "account_created_time"]], on="user_id", how="left")

    df["account_created_time"] = pd.to_datetime(df["account_created_time"])
    df["account_age_days"] = (datetime.utcnow() - df["account_created_time"]).dt.days

    df["total_karma_growth"] = df["post_karma_growth"] + df["comment_karma_growth"]

    df["f1_karma_growth"] = normalize(df["total_karma_growth"])
    df["f2_activity"] = normalize(df["num_posts_last_30_days"])
    df["f3_quality"] = normalize(df["avg_score_last_30_days"])
    df["f4_push"] = normalize(df["successful_pushes"])
    df["f5_age"] = normalize(df["account_age_days"])

    # 双向分数模型
    # def rule_based_score(profile):
    #     score = 0
    #     # 正向
    #     score += min(profile['total_karma'] / 1_000, 20)          # 0-20
    #     score += min(profile['hot_post_count'] * 2, 20)           # 0-20
    #     score += 15 if profile['fact_pass_rate'] > 0.8 else 0     # 0/15
    #     # 负向
    #     score -= 20 if profile['bot_score'] > 0.7 else 0
    #     score -= 10 if profile['deleted_rate'] > 0.3 else 0
    #     # 裁剪到 0-100
    #     return max(0, min(score, 100))

    df["influence_score"] = (
        0.20 * df["f1_karma_growth"] +
        0.20 * df["f2_activity"] +
        0.35 * df["f3_quality"] +
        0.20 * df["f4_push"] +
        0.05 * df["f5_age"]
    ) * 100

    # 四舍五入
    df["influence_score"] = df["influence_score"].round(2)

    df_out = df[["user_id", "user_name", "influence_score"]].sort_values(by="influence_score", ascending=False)
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    print("score file saved to", output_path)
    return df_out.head()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--static", type=str, default="user_static_info.json", help="静态信息输入路径")
    parser.add_argument("--dynamic", type=str, default="user_dynamic_info.json", help="动态信息输入路径")
    parser.add_argument("--output", type=str, default="user_influence_scores.csv", help="输出 CSV 文件路径")
    args = parser.parse_args()

    top = compute_influence_scores(
        static_path=args.static,
        dynamic_path=args.dynamic,
        output_path=args.output
    )

    print("top users:")
    print(top)

if __name__ == "__main__":
    main()

