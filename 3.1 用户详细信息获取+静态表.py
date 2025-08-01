import praw
import pandas as pd
import json
from datetime import datetime
import os
import argparse

# 1. 配置 Reddit API
reddit = praw.Reddit(
    client_id="SfT7qiMbOcpFpLpcjr2mLA",
    client_secret="13u1kmmbFA-lJPHWDxpz78BMyLvAsQ",
    user_agent="Even-Dare-8131"
)

def main(user_pool_file, static_file):
    # === 1. 读取已有静态用户信息（如存在）===
    existing_users = set()
    existing_data = []

    if os.path.exists(static_file):
        with open(static_file, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line.strip())
                existing_users.add(entry["user_id"])
                existing_data.append(entry)

    # === 2. 读取原始用户池（新用户）===
    with open(user_pool_file, "r", encoding="utf-8") as f:
        user_pool = [json.loads(line.strip()) for line in f]

    # === 3. 仅抓取未入库的用户信息 ===
    user_static_info = []

    for user in user_pool:
        username = user["author_name"]
        user_id = user["author_id"]

        if user_id in existing_users:
            continue  # 跳过已存在用户

        try:
            redditor = reddit.redditor(username)
            static_data = {
                "user_id": user_id,
                "user_name": username,
                "collected_time": datetime.now().isoformat(),  # 入库时间
                "account_created_time": datetime.utcfromtimestamp(redditor.created_utc).isoformat(),  # 注册时间
                "initial_post_karma": redditor.link_karma,   # 入库时帖子 karma
                "initial_comment_karma": redditor.comment_karma  # 入库时评论 karma
            }

            user_static_info.append(static_data)
            existing_users.add(user_id)  # 确保不重复
        except Exception as e:
            print(f"获取 {username} 信息失败: {e}")

    # === 4. 追加写入新抓取的用户 ===
    with open(static_file, "a", encoding="utf-8") as f:
        for entry in user_static_info:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"本轮新增入库用户：{len(user_static_info)} 条")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="users_raw_pool.json", help="用户池文件路径")
    parser.add_argument("--output", type=str, default="user_static_info.json", help="输出静态信息文件路径")
    args = parser.parse_args()

    main(user_pool_file=args.input, static_file=args.output)
