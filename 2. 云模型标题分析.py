import json
import pandas as pd
import re
import asyncio
import aiohttp
import argparse

DASHSCOPE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
API_KEY = "sk-3a86f5d46b844c73b4e1f6eec6e4fef5"
MODEL_NAME = "deepseek-r1-distill-llama-70b"

# 控制并发量和最小请求间隔（秒）
semaphore = asyncio.Semaphore(5)
MIN_REQUEST_INTERVAL = 0.2

def build_prompt(title):
    return f"""
你是一个行业分析助手，你的任务是判断一个帖子标题是否与下列领域具有直接或间接的关联：

- 制造业
- 进出口贸易
- 供应链
- 物流运输
- 出口或进口政策、法规

请根据你的理解，结合语义判断该标题是否可能涉及上述任一主题。

请严格遵循以下输出格式：
1. 如果你认为相关，请输出：是(置信度)
2. 如果你认为不相关，请输出：否(置信度)
3. 置信度必须是 0 到 1 之间的数字，小数点后保留两位
4. 不要输出其他解释、理由或额外内容，只输出“是(置信度)”或“否(置信度)”
5. 置信度越高，说明判断正确的概率越高

下面是要判断的标题：
标题：{title}
"""

async def analyze_title(session, title, index):
    prompt = build_prompt(title)
    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}]
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    retry = 0
    while retry < 3:
        async with semaphore:
            try:
                await asyncio.sleep(MIN_REQUEST_INTERVAL)
                async with session.post(DASHSCOPE_URL, headers=headers, json=payload, timeout=60) as resp:
                    data = await resp.json()

                    if "error" in data and data["error"].get("code") == "limit_requests":
                        retry += 1
                        wait_time = 2 ** retry
                        print(f"[{index}] 触发限流，第{retry}次重试，等待{wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                    if "choices" not in data or not data["choices"]:
                        print(f"[{index}] 响应无choices字段: {data}")
                        return index, None, 0.0

                    content = data["choices"][0]["message"]["content"].strip()
                    if "</think>" in content:
                        content = content.split("</think>")[-1].strip()

                    match = re.match(r"^(是|否)\s*\(\s*([0-1](?:\.\d{1,2})?)\s*\)$", content)
                    if match:
                        label = match.group(1)
                        confidence = float(match.group(2))
                        related = label == "是"
                        return index, related, confidence
                    else:
                        print(f"[{index}] 模型输出无法解析: '{content}'")
                        return index, None, 0.0

            except Exception as e:
                retry += 1
                wait_time = 2 ** retry
                print(f"[{index}] 请求异常: {e}，重试第{retry}次，等待{wait_time}s")
                await asyncio.sleep(wait_time)

    return index, None, 0.0

async def main_async(input_file, output_file):
    df = pd.read_json(input_file, lines=True)
    df = df.head(100).copy()
    df["industry_related"] = None
    df["confidence"] = None
    total = len(df)

    timeout = aiohttp.ClientTimeout(total=70)
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [analyze_title(session, row["title"], i) for i, row in df.iterrows()]
        completed = 0
        for coro in asyncio.as_completed(tasks):
            index, related, confidence = await coro
            df.at[index, "industry_related"] = related
            df.at[index, "confidence"] = round(confidence, 2)
            completed += 1
            status = "相关" if related else ("不相关" if related is False else "无法判断")
            title = df.at[index, "title"]
            print(f"[{completed}/{total}] {title} --> {status}, confidence={confidence:.2f}")

    relevant = df[(df["industry_related"] == True) & (df["confidence"] >= 0.7)]
    relevant.to_json(output_file, orient="records", lines=True, force_ascii=False)

    print("\n相关帖子（置信度 ≥ 0.70）：")
    print(relevant[["title", "url", "subreddit", "confidence"]])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="100_hot_posts.json")
    parser.add_argument("--output", type=str, default="related_posts.json")
    args = parser.parse_args()
    asyncio.run(main_async(args.input, args.output))
