"""
Stage 1 of the pipeline: clean the raw places dataset and synthesise users +
ratings so that Part B (recommendation) has interaction data to work with.

The raw CSV only contains place metadata (no users, no ratings). To make
collaborative filtering possible we generate synthetic users with a `home
district` and `preferred types`, then sample ratings biased toward those
preferences (and lightly toward Grade-A places). The synthesis is clearly
labelled in the README/slides; it is not real user behaviour.
"""
from __future__ import annotations

import random
import re
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from spark_env import get_spark


ROOT = Path(__file__).resolve().parent.parent
RAW_CSV = ROOT / "Places for Travel-Dining-Recreational activities and Information of travel agents.csv"
DATA_DIR = ROOT / "data"

# Keyword -> tag rules. Tags are derived from the place Name so the content-based
# recommender has something richer than just `Type` to compare on.
TAG_RULES: list[tuple[str, str]] = [
    (r"\b(beach|sea|bay|coral|coast|hikkaduwa|bentota|negombo)\b", "beach"),
    (r"\b(thai|chinese|japanese|indian|italian|korean|french|asian)\b", "asian-cuisine"),
    (r"\b(grill|bbq|barbecue|steak)\b", "grill"),
    (r"\b(bakery|bake|cafe|coffee|tea)\b", "cafe"),
    (r"\b(spa|ayurveda|wellness|massage|herbal)\b", "wellness"),
    (r"\b(spice|garden|herb)\b", "garden"),
    (r"\b(dive|diving|surf|water|jet|ski|raft|boat)\b", "watersport"),
    (r"\b(travel|tour|tours|holiday|vacation|trip|expedition)\b", "tour-operator"),
    (r"\b(hotel|resort|inn|lodge|hostel|villa)\b", "stay"),
    (r"\b(handicraft|craft|gem|silver|gift|shop|store|art|batik)\b", "shopping"),
    (r"\b(restaurant|kitchen|food|dine|dining|bistro|eatery)\b", "restaurant"),
    (r"\b(pub|bar|whiskey|wine|cocktail)\b", "nightlife"),
]


def derive_tags(name: str, type_: str) -> str:
    text = f"{name or ''} {type_ or ''}".lower()
    found = {tag for pattern, tag in TAG_RULES if re.search(pattern, text)}
    type_tag = (type_ or "unknown").lower().strip().replace(" ", "-").replace("&", "and")
    found.add(type_tag)
    return " ".join(sorted(found))


def derive_city(address: str) -> str:
    if not address:
        return "Unknown"
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if not parts:
        return "Unknown"
    last = re.sub(r"\s*\d+\s*$", "", parts[-1]).strip()
    return last.title() if last else "Unknown"


def clean_places(spark):
    print(f"[data_prep] Loading raw CSV: {RAW_CSV.name}")
    raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(RAW_CSV))
    )
    print(f"[data_prep] Raw row count: {raw.count()}")

    derive_tags_udf = F.udf(derive_tags, StringType())
    derive_city_udf = F.udf(derive_city, StringType())

    cleaned = (
        raw.withColumn("Name", F.trim(F.col("Name")))
        .withColumn("Type", F.trim(F.col("Type")))
        .withColumn("Address", F.trim(F.col("Address")))
        .withColumn("District", F.coalesce(F.trim(F.col("District")), F.lit("Unknown")))
        .withColumn(
            "Grade",
            F.when(F.col("Grade").isNull(), F.lit("Unrated")).otherwise(F.col("Grade")),
        )
        .withColumn("City", derive_city_udf(F.col("Address")))
        .withColumn("Tags", derive_tags_udf(F.col("Name"), F.col("Type")))
        .dropDuplicates(["Name", "Address"])
    )

    cleaned = cleaned.withColumn(
        "place_id",
        F.row_number().over(Window.orderBy("Name", "Address")),
    )

    print(f"[data_prep] Cleaned row count: {cleaned.count()}")
    return cleaned


def synthesize_users_and_ratings(places_pdf, n_users=300, n_ratings=5000, seed=42):
    """Synthesise a users table and a ratings table biased by user preferences.
    Driver-side because the cleaned places frame is small (~1.5k rows)."""
    rng = random.Random(seed)
    districts = sorted(places_pdf["District"].dropna().unique().tolist())
    types = sorted(places_pdf["Type"].dropna().unique().tolist())

    users = []
    for uid in range(1, n_users + 1):
        home = rng.choice(districts)
        prefs = rng.sample(types, k=min(2, len(types)))
        users.append(
            {
                "user_id": uid,
                "home_district": home,
                "preferred_types": "|".join(prefs),
            }
        )

    ratings = []
    records = places_pdf.to_dict("records")
    place_count = len(records)

    for _ in range(n_ratings):
        user = users[rng.randrange(n_users)]
        prefs = set(user["preferred_types"].split("|"))
        # Up to 8 acceptance attempts per rating: preferred type +3, same
        # district +2, Grade A +1, base 1. Avoids materialising a full
        # per-user distribution over places.
        weight = 1
        place = records[rng.randrange(place_count)]
        for _attempt in range(8):
            cand = records[rng.randrange(place_count)]
            w = 1
            if cand["Type"] in prefs:
                w += 3
            if cand["District"] == user["home_district"]:
                w += 2
            if cand["Grade"] == "A":
                w += 1
            if rng.random() * 7 < w:
                place, weight = cand, w
                break

        base = 2.5 + 0.4 * weight + rng.gauss(0, 0.6)
        rating = max(1.0, min(5.0, round(base * 2) / 2))
        ratings.append(
            {
                "user_id": user["user_id"],
                "place_id": int(place["place_id"]),
                "rating": float(rating),
            }
        )

    return users, ratings


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("BDA_DataPrep")

    cleaned = clean_places(spark)
    places_pdf = cleaned.toPandas()

    out_places = DATA_DIR / "places_clean.csv"
    places_pdf.to_csv(out_places, index=False)
    print(f"[data_prep] Wrote {out_places} ({len(places_pdf)} rows)")

    users, ratings = synthesize_users_and_ratings(places_pdf)
    import pandas as pd

    pd.DataFrame(users).to_csv(DATA_DIR / "users.csv", index=False)
    pd.DataFrame(ratings).to_csv(DATA_DIR / "ratings.csv", index=False)
    print(f"[data_prep] Wrote users.csv ({len(users)} users)")
    print(f"[data_prep] Wrote ratings.csv ({len(ratings)} ratings)")

    spark.stop()


if __name__ == "__main__":
    main()
