import json
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import argparse

def cluster_posts(input_path="post_tracking.json",
                  output_path="post_clusters_dbscan.csv",
                  noise_output_path="potential_hotposts.csv"):
    # 读取 JSON 数据
    records = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    df = pd.DataFrame(records)

    # 选择用于聚类的特征
    features = [
        "time_since_post_hours",
        "current_score",
        "current_comments",
        "score_growth",
        "comment_growth",
        "unit_score_growth",
        "unit_comment_growth",
        "total_score_speed"
    ]
    X = df[features].fillna(0)

    # 标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # PCA 降维用于可视化
    pca = PCA(n_components=2)
    pcs = pca.fit_transform(X_scaled)
    df["PC1"], df["PC2"] = pcs[:, 0], pcs[:, 1]

    # DBSCAN 聚类
    dbscan = DBSCAN(eps=1, min_samples=5)
    df["cluster"] = dbscan.fit_predict(X_scaled)
    df["label"] = df["cluster"].apply(lambda x: "Noise" if x == -1 else f"Cluster_{x}")

    # 输出聚类结果
    output_columns = [
        "post_id", "post_title", "user_name",
        "time_since_post_hours", "current_score", "current_comments",
        "score_growth", "comment_growth",
        "unit_score_growth", "unit_comment_growth", "total_score_speed",
        "label","url"
    ]
    df_out = df[output_columns].sort_values(by=["label", "post_id"])
    df_out.to_csv(output_path, index=False, encoding="utf-8")
    print("聚类结果已保存到", output_path)

    # 提取 Noise 样本
    noise_df = df_out[df_out["label"] == "Noise"]
    noise_df.to_json(noise_output_path, orient="records", lines=True, force_ascii=False)
    print("潜在隐藏热点帖已保存到", noise_output_path)

    # 可视化
    plt.figure(figsize=(8, 6))
    for label, grp in df.groupby("label"):
        plt.scatter(grp["PC1"], grp["PC2"], label=label, s=50, alpha=0.7)
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.title("PCA 2D + DBSCAN")
    plt.legend()
    plt.tight_layout()
    plt.show()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="post_tracking.json", help="输入 JSON 文件路径")
    parser.add_argument("--output", type=str, default="post_clusters_dbscan.csv", help="输出聚类结果 CSV 路径")
    parser.add_argument("--noise_output", type=str, default="potential_hotposts.json", help="输出噪声样本 CSV 路径")
    args = parser.parse_args()

    cluster_posts(input_path=args.input,
                  output_path=args.output,
                  noise_output_path=args.noise_output)

if __name__ == "__main__":
    main()
