"""
Part A: Big Data Analytics with Apache Spark.

Produces a single JSON artefact (`outputs/analytics.json`) that the Flask API
serves to the frontend. All heavy lifting is done in Spark; only the final
small aggregate frames are pulled to the driver.

Pipeline:
  1. Load cleaned places.
  2. EDA aggregates: type distribution, district distribution, grade mix.
  3. District tourism profile: counts of each Type per District.
  4. K-Means clustering on the district profile -> tourism-personality clusters.
  5. Tag frequency analysis (text features derived in data_prep).
  6. Per-district summary table for the dashboard map/table.
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import functions as F

from spark_env import get_spark


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "outputs"


def load_places(spark):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(DATA_DIR / "places_clean.csv"))
    )


def type_distribution(places):
    return (
        places.groupBy("Type")
        .count()
        .orderBy(F.desc("count"))
        .toPandas()
        .to_dict("records")
    )


def district_distribution(places, top_n=15):
    return (
        places.groupBy("District")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
        .toPandas()
        .to_dict("records")
    )


def grade_distribution(places):
    return (
        places.groupBy("Grade")
        .count()
        .orderBy(F.desc("count"))
        .toPandas()
        .to_dict("records")
    )


def grade_by_type(places):
    """How does grade vary by place type? (Only Restaurants are graded today.)"""
    return (
        places.groupBy("Type", "Grade")
        .count()
        .orderBy("Type", F.desc("count"))
        .toPandas()
        .to_dict("records")
    )


def district_profile_pivot(places):
    """One row per district, one column per Type, value = count of places."""
    return (
        places.groupBy("District")
        .pivot("Type")
        .count()
        .na.fill(0)
    )


def cluster_districts(profile_df, k=4, seed=42):
    """K-Means on a standardised district-profile vector. Districts in the same
    cluster have similar tourism mixes (e.g. coastal-heavy vs urban-restaurant)."""
    feature_cols = [c for c in profile_df.columns if c != "District"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    kmeans = KMeans(featuresCol="features", k=k, seed=seed)

    assembled = assembler.transform(profile_df)
    scaled = scaler.fit(assembled).transform(assembled)
    model = kmeans.fit(scaled)
    clustered = model.transform(scaled).select("District", "prediction", *feature_cols)

    # Cluster labels: pick the dominant Type in each cluster as a human label.
    label_map = {}
    cluster_records = clustered.toPandas()
    for cid, group in cluster_records.groupby("prediction"):
        sums = group[feature_cols].sum().sort_values(ascending=False)
        dominant = sums.index[0]
        label_map[int(cid)] = f"{dominant}-heavy"

    cluster_records["cluster_label"] = cluster_records["prediction"].map(label_map)
    return cluster_records.to_dict("records"), label_map, model.summary.trainingCost


def tag_frequency(places, top_n=15):
    tokens = places.select(F.explode(F.split(F.col("Tags"), " ")).alias("tag"))
    return (
        tokens.filter(F.length("tag") > 0)
        .groupBy("tag")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
        .toPandas()
        .to_dict("records")
    )


def district_summary(places):
    """Per-district summary: total places, dominant type, grade-A share."""
    total = places.groupBy("District").count().withColumnRenamed("count", "total")
    dominant = (
        places.groupBy("District", "Type")
        .count()
        .withColumn(
            "rank",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("District").orderBy(F.desc("count"))
            ),
        )
        .filter(F.col("rank") == 1)
        .select("District", F.col("Type").alias("dominant_type"))
    )
    grade_a = (
        places.filter(F.col("Grade") == "A")
        .groupBy("District")
        .count()
        .withColumnRenamed("count", "grade_a_count")
    )

    summary = (
        total.join(dominant, "District", "left")
        .join(grade_a, "District", "left")
        .na.fill({"grade_a_count": 0})
        .orderBy(F.desc("total"))
    )
    return summary.toPandas().to_dict("records")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("BDA_PartA")

    places = load_places(spark).cache()
    n = places.count()
    print(f"[part_a] Loaded {n} cleaned places")

    artefact = {
        "n_places": n,
        "type_distribution": type_distribution(places),
        "district_distribution": district_distribution(places),
        "grade_distribution": grade_distribution(places),
        "grade_by_type": grade_by_type(places),
        "tag_frequency": tag_frequency(places),
        "district_summary": district_summary(places),
    }

    profile = district_profile_pivot(places)
    cluster_records, label_map, training_cost = cluster_districts(profile, k=4)
    artefact["clusters"] = {
        "labels": label_map,
        "training_cost": training_cost,
        "districts": cluster_records,
    }

    out = OUT_DIR / "analytics.json"
    out.write_text(json.dumps(artefact, indent=2, default=str), encoding="utf-8")
    print(f"[part_a] Wrote {out}")

    # Print a few headline numbers so the run is self-explanatory in logs.
    print("[part_a] Headline insights:")
    print(f"   - Total places: {n}")
    print(f"   - Distinct districts: {len(artefact['district_summary'])}")
    print(f"   - Top type: {artefact['type_distribution'][0]}")
    print(f"   - Cluster labels: {label_map}")

    spark.stop()


if __name__ == "__main__":
    main()
