import json
import time
from datetime import datetime, timedelta
import praw
import argparse
import os

reddit = praw.Reddit(
    client_id="SfT7qiMbOcpFpLpcjr2mLA",
    client_secret="13u1kmmbFA-lJPHWDxpz78BMyLvAsQ",
    user_agent="Even-Dare-8131"
)

# 4. 获取近 30 天发帖信息
def get_recent_posts_info(user_name, since):
    redditor = reddit.redditor(user_name)
    count = 0
    score_total = 0
    last_post_time = None

    try:
        for submission in redditor.submissions.new(limit=100):
            created_time = datetime.utcfromtimestamp(submission.created_utc)
            if created_time >= since:
                count += 1
                score_total += submission.score
                if not last_post_time or created_time > last_post_time:
                    last_post_time = created_time
    except Exception as e:
        print(f"获取 {user_name} 的发帖失败：{e}")

    return count, score_total, last_post_time.isoformat() if last_post_time else None

def main(static_file, dynamic_file):
    # 1. 读取静态表
    with open(static_file, "r", encoding="utf-8") as f:
        static_users = [json.loads(line.strip()) for line in f]

    # 2. 读取已有动态库（如果存在）
    try:
        with open(dynamic_file, "r", encoding="utf-8") as f:
            dynamic_users = {json.loads(line)["user_id"]: json.loads(line) for line in f}
    except FileNotFoundError:
        dynamic_users = {}

    # 3. 时间范围定义（近 30 天）
    now = datetime.utcnow()
    since = now - timedelta(days=30)

    # 5. 遍历每个静态用户，生成动态记录 + 派生变量
    updated_records = []

    for user in static_users:
        user_id = user["user_id"]
        user_name = user["user_name"]

        try:
            redditor = reddit.redditor(user_name)
            post_karma = redditor.link_karma
            comment_karma = redditor.comment_karma
            post_count_30d, score_total_30d, last_post_time = get_recent_posts_info(user_name, since)

            # 派生计算（基于 static_data）
            initial_post_karma = user.get("initial_post_karma", 0)
            initial_comment_karma = user.get("initial_comment_karma", 0)
            account_created_time = datetime.fromisoformat(user["account_created_time"])
            account_age_days = (now - account_created_time).days or 1  # 防止除0

            post_karma_growth = post_karma - initial_post_karma
            comment_karma_growth = comment_karma - initial_comment_karma
            total_karma_growth = post_karma_growth + comment_karma_growth

            dynamic_data = {
                "user_id": user_id,
                "user_name": user_name,
                "current_post_karma": post_karma,
                "current_comment_karma": comment_karma,
                "num_posts_last_30_days": post_count_30d,
                "last_post_time": last_post_time,
                "avg_score_last_30_days": round(score_total_30d / post_count_30d, 2) if post_count_30d else 0,
                "successful_pushes": dynamic_users.get(user_id, {}).get("successful_pushes", 0),

                # 新增的派生变量
                "post_karma_growth": post_karma_growth,
                "comment_karma_growth": comment_karma_growth,
                "total_karma_growth": total_karma_growth,
                "account_age_days": account_age_days
            }

            updated_records.append(dynamic_data)
            print(f"已更新用户：{user_name}")
            time.sleep(1)  # 限制速率

        except Exception as e:
            print(f"获取 {user_name} 失败: {e}")

    # 6. 写入动态信息表
    with open(dynamic_file, "w", encoding="utf-8") as f:
        for entry in updated_records:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"\n共更新 {len(updated_records)} 条动态用户信息")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="user_static_info.json", help="静态信息输入路径")
    parser.add_argument("--output", type=str, default="user_dynamic_info.json", help="动态信息输出路径")
    args = parser.parse_args()

    main(static_file=args.input, dynamic_file=args.output)
