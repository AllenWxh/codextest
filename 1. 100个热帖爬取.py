import praw
import pandas as pd
import argparse

# reddit API
def fetch_hot_posts(output_file, limit=100):
    reddit = praw.Reddit(
        client_id="SfT7qiMbOcpFpLpcjr2mLA",
        client_secret="13u1kmmbFA-lJPHWDxpz78BMyLvAsQ",
        user_agent="Even-Dare-8131"
    )

    top_posts = reddit.subreddit('popular').hot(limit=limit)

    # 把帖子对象先保存到列表中
    posts_data = []
    for post in top_posts:
        author_obj = post.author            # 可能是 None（账号被删）
        author_name = author_obj.name if author_obj else None

        # Reddit API 不直接给数值型 user_id，通常用 base‑36 id
        author_id = None
        if author_obj and hasattr(author_obj, "id"):
            author_id = author_obj.id       # 如 't2_abcd123'

        posts_data.append({
            "title":        post.title,
            "score":        post.score,
            "url":          post.url,
            "subreddit":    post.subreddit.display_name,
            "id":           post.id,
            "created_utc":  post.created_utc,
            "author_name":  author_name,
            "author_id":    author_id
        })

    # 创建 DataFrame
    df = pd.DataFrame(posts_data)

    # 保存为 JSON 文件
    df.to_json(output_file, orient="records", lines=True, force_ascii=False)
    print('已成功爬取当前时刻100个热点帖子并保存')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, default="100_hot_posts.json", help="输出文件名")
    parser.add_argument("--limit", type=int, default=100, help="抓取帖子数量")
    args = parser.parse_args()

    fetch_hot_posts(output_file=args.output, limit=args.limit)

# 例如可以用
# python 1. 100个热帖爬取.py --output output.json --limit 50
