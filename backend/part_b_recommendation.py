"""
Part B: Big Data Recommendation System using Apache Spark.

Trains an ALS Collaborative Filtering model on the synthesized user ratings,
and pre-computes the top 10 recommended places for every user.
Outputs a single JSON file `outputs/recommendations.json` for the Flask API.
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from spark_env import get_spark


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "outputs"


def load_data(spark):
    ratings = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "ratings.csv"))
    places = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "places_clean.csv"))
    return ratings, places


def train_model(ratings):
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_id",
        itemCol="place_id",
        ratingCol="rating",
        coldStartStrategy="drop"
    )
    model = als.fit(ratings)
    return model


def generate_recommendations(model, places):
    # Get top 10 recommendations for all users
    user_recs = model.recommendForAllUsers(10)
    
    # Explode the recommendations to join with places metadata
    exploded = user_recs.withColumn("rec", F.explode("recommendations")).select(
        "user_id", 
        F.col("rec.place_id").alias("place_id"), 
        F.col("rec.rating").alias("pred_rating")
    )
    
    # Join with place details
    joined = exploded.join(places, "place_id", "left").orderBy("user_id", F.desc("pred_rating"))
    
    # Aggregate back to list of dicts per user
    records = joined.toPandas()
    
    output = {}
    for user_id, group in records.groupby("user_id"):
        # Select only the relevant place metadata
        recs = group[["place_id", "Name", "Type", "City", "District", "Tags", "pred_rating"]].to_dict("records")
        output[int(user_id)] = recs
        
    return output


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("BDA_PartB")

    print("[part_b] Loading data...")
    ratings, places = load_data(spark)
    
    print("[part_b] Training ALS model...")
    model = train_model(ratings)
    
    print("[part_b] Generating top 10 recommendations for all users...")
    recommendations_dict = generate_recommendations(model, places)
    
    out = OUT_DIR / "recommendations.json"
    out.write_text(json.dumps(recommendations_dict, indent=2, default=str), encoding="utf-8")
    print(f"[part_b] Wrote {out} for {len(recommendations_dict)} users.")

    spark.stop()


if __name__ == "__main__":
    main()
