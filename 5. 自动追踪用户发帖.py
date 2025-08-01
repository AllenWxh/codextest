import json
import os
from datetime import datetime, timedelta
import argparse
import praw

def load_users(user_file):
    users = []
    if not os.path.exists(user_file):
        return users
    with open(user_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                users.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return users

def load_tracking(track_file):
    tracking = {}
    if not os.path.exists(track_file):
        return tracking
    with open(track_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                pid = rec.get("post_id")
                if pid:
                    tracking[pid] = rec
            except json.JSONDecodeError:
                continue
    return tracking

def save_tracking(tracking, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        for rec in tracking.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def get_recent_posts(username, since_time, reddit):
    results = []
    try:
        redditor = reddit.redditor(username)
        for submission in redditor.submissions.new(limit=100):
            post_time = datetime.utcfromtimestamp(submission.created_utc)
            if post_time < since_time:
                break
            results.append({
                "post_id": submission.id,
                "post_title": submission.title,
                "created_time": post_time.isoformat(),
                "score": submission.score,
                "comments": submission.num_comments,
                "url": submission.url
            })
    except Exception as e:
        print("获取用户帖子失败", username, e)
    return results

def track_posts(user_file, track_file, hours, client_id, client_secret, user_agent):
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )

    now = datetime.utcnow()
    cutoff = now - timedelta(hours=hours)

    users = load_users(user_file)
    tracking = load_tracking(track_file)

    for user in users:
        uid = user.get("user_id")
        uname = user.get("user_name")
        if not uid or not uname:
            continue

        posts = get_recent_posts(uname, cutoff, reddit)
        for post in posts:
            pid = post["post_id"]
            title = post["post_title"]
            created_time = datetime.fromisoformat(post["created_time"])
            hours_since_post = (now - created_time).total_seconds() / 3600
            curr_score = post["score"]
            curr_comments = post["comments"]

            if pid not in tracking:
                tracking[pid] = {
                    "post_id": pid,
                    "post_title": title,
                    "user_id": uid,
                    "user_name": uname,
                    "url": post.get("url"),
                    "created_time": post["created_time"],
                    "time_since_post_hours": round(hours_since_post, 2),
                    "current_score": curr_score,
                    "current_comments": curr_comments,
                    "last_score": curr_score,
                    "last_comments": curr_comments,
                    "score_growth": 0,
                    "comment_growth": 0,
                    "unit_score_growth": 0,
                    "unit_comment_growth": 0,
                    "total_score_speed": round(curr_score / hours_since_post, 2) if hours_since_post > 0 else 0,
                    "last_checked": now.isoformat()
                }
            else:
                rec = tracking[pid]
                prev_score = rec.get("last_score", 0)
                prev_comments = rec.get("last_comments", 0)
                last_checked_time = datetime.fromisoformat(rec.get("last_checked"))
                interval_hours = (now - last_checked_time).total_seconds() / 3600

                score_growth = curr_score - prev_score
                comment_growth = curr_comments - prev_comments
                unit_score_growth = round(score_growth / interval_hours, 2) if interval_hours > 0 else 0
                unit_comment_growth = round(comment_growth / interval_hours, 2) if interval_hours > 0 else 0
                total_score_speed = round(curr_score / hours_since_post, 2) if hours_since_post > 0 else 0

                rec["post_title"] = title
                rec["url"] = post.get("url")
                rec["time_since_post_hours"] = round(hours_since_post, 2)
                rec["current_score"] = curr_score
                rec["current_comments"] = curr_comments
                rec["score_growth"] = score_growth
                rec["comment_growth"] = comment_growth
                rec["unit_score_growth"] = unit_score_growth
                rec["unit_comment_growth"] = unit_comment_growth
                rec["total_score_speed"] = total_score_speed
                rec["last_score"] = curr_score
                rec["last_comments"] = curr_comments
                rec["last_checked"] = now.isoformat()

    # 清理时间窗口外的记录
    new_tracking = {
        pid: rec for pid, rec in tracking.items()
        if datetime.fromisoformat(rec["created_time"]) >= cutoff
    }

    save_tracking(new_tracking, track_file)
    print("追踪完成，有效帖子数：", len(new_tracking))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user_file", type=str, default="user_static_info.json", help="用户静态信息文件")
    parser.add_argument("--track_file", type=str, default="post_tracking.json", help="帖子追踪输出文件")
    parser.add_argument("--window_hours", type=int, default=24, help="追踪时间窗口（小时）")
    parser.add_argument("--client_id", type=str, default="SfT7qiMbOcpFpLpcjr2mLA")
    parser.add_argument("--client_secret", type=str, default="13u1kmmbFA-lJPHWDxpz78BMyLvAsQ")
    parser.add_argument("--user_agent", type=str, default="Even-Dare-8131")
    args = parser.parse_args()

    track_posts(
        user_file=args.user_file,
        track_file=args.track_file,
        hours=args.window_hours,
        client_id=args.client_id,
        client_secret=args.client_secret,
        user_agent=args.user_agent
    )

if __name__ == "__main__":
    main()
