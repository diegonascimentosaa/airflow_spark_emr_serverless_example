import os
import json
from pyspark.sql import functions as F
from etl.utils import get_api_data, write_parquet


class ETLStep:
    BASE_PATH = "/app/data"

    def __init__(self, spark, args):
        self.spark = spark
        self.args = args
        self.endpoints = ["users", "posts", "comments"]

    def _get_path(self, layer, name, ext=""):
        filename = f"{name}_{self.args.exec_date}{ext}" if ext else name
        return os.path.join(self.BASE_PATH, layer, filename)

    def _read_parquet(self, layer, name):
        return self.spark.read.parquet(self._get_path(layer, name))

    def execute(self):
        raise NotImplementedError


class IngestionStep(ETLStep):
    def execute(self):
        os.makedirs(os.path.join(self.BASE_PATH, "transient"), exist_ok=True)

        for endpoint in self.endpoints:
            print(f"⬇️ Downloading: {endpoint}")
            data = get_api_data(endpoint)

            path = self._get_path("transient", endpoint, ".json")
            with open(path, "w") as f:
                json.dump(data, f)
            print(f"✅ Saved in: {path}")


class RawStep(ETLStep):
    def execute(self):
        for endpoint in self.endpoints:
            path_in = self._get_path("transient", endpoint, ".json")

            if not os.path.exists(path_in):
                raise FileNotFoundError(f"File not found: {path_in}")

            print(f"⚙️ Processing RAW: {endpoint}")
            df = self.spark.read.json(path_in)
            write_parquet(df, "raw", endpoint)


class TrustedStep(ETLStep):
    def execute(self):
        print("🔨 Running Trusted...")

        df_users = self._read_parquet("raw", "users").select(
            F.col("id").alias("user_id"),
            F.col("name").alias("user_name"),
            F.col("email").alias("user_email"),
        )
        write_parquet(df_users, "trusted", "users")

        df_posts = self._read_parquet("raw", "posts").select(
            F.col("id").alias("post_id"),
            F.col("userId").alias("post_user_id"),
            F.col("title").alias("post_title"),
            F.col("body").alias("post_body"),
        )
        write_parquet(df_posts, "trusted", "posts")

        df_comments = self._read_parquet("raw", "comments").select(
            F.col("id").alias("comment_id"),
            F.col("postId").alias("comment_post_id"),
            F.col("name").alias("comment_name"),
            F.col("email").alias("comment_email"),
            F.col("body").alias("comment_body"),
        )
        write_parquet(df_comments, "trusted", "comments")


class RefinedStep(ETLStep):
    def execute(self):
        print("💎 Running Refined...")

        u = self._read_parquet("trusted", "users")
        p = self._read_parquet("trusted", "posts")
        c = self._read_parquet("trusted", "comments")

        df_final = (
            u.join(p, u.user_id == p.post_user_id, "inner")
            .join(c, p.post_id == c.comment_post_id, "inner")
            .select(
                "user_name", "user_email", "post_title", "comment_name", "comment_body"
            )
        )

        write_parquet(df_final, "refined", "relatorio_final")
        df_final.show(5, truncate=False)
