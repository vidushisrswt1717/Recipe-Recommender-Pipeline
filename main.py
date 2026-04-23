import json
import pandas as pd
import sqlite3
import re
import ast
import logging
import os
from datetime import datetime

logging.basicConfig(filename="pipeline.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

FRIDGE_FILE = "Fridge_Items.json"
RECIPE_FILE = "recipes.csv"
DB_FILE = "recipes.db"

SYNONYMS = {
    "tomatoes": "tomato", "potatoes": "potato", "onions": "onion",
    "eggs": "egg", "chilies": "chili", "chillies": "chili", "peppers": "pepper"
}

SUBSTITUTIONS = {
    "butter": "margarine", "cream": "milk", "lemon": "lime"
}


def extract(fridge_path, recipe_path):
    print("Extracting data...")
    try:
        f = open(fridge_path, 'r')
        data = json.load(f)
        f.close()
        fridge_items = data["ingredients"]
        print(f"Fridge items loaded: {fridge_items}")
        logging.info("fridge loaded")
    except Exception as e:
        print(f"Error loading fridge: {e}")
        logging.error(str(e))
        return None, None

    try:
        recipes_df = pd.read_csv(recipe_path)
        print(f"Recipes loaded: {len(recipes_df)} recipes found")
        logging.info("recipes loaded")
    except Exception as e:
        print(f"Error loading recipes: {e}")
        logging.error(str(e))
        return None, None

    return fridge_items, recipes_df


def normalize(ingredient):
    ingredient = ingredient.lower().strip()
    ingredient = re.sub(r'\d+\s*(g|kg|ml|l|cups?|tbsp|tsp|oz|lb|grams?)\b', '', ingredient)
    ingredient = re.sub(r'\d+', '', ingredient).strip()
    return SYNONYMS.get(ingredient, ingredient)


def transform(fridge_items, recipes_df):
    print("Transforming data...")

    clean_fridge = [normalize(i) for i in fridge_items if normalize(i) != ""]

    scores = []
    missing_list = []

    for index, row in recipes_df.iterrows():
        try:
            ing_list = ast.literal_eval(row["ingredients"])
        except:
            ing_list = []

        clean_recipe = [normalize(i) for i in ing_list if normalize(i) != ""]

        fridge_set = set(clean_fridge)
        recipe_set = set(clean_recipe)
        matched = fridge_set.intersection(recipe_set)
        missing = recipe_set - fridge_set

        still_missing = []
        for item in missing:
            has_sub = any(
                (item == orig and sub in fridge_set) or (item == sub and orig in fridge_set)
                for orig, sub in SUBSTITUTIONS.items()
            )
            if not has_sub:
                still_missing.append(item)

        score = (len(matched) / len(recipe_set)) * 100 if recipe_set else 0
        scores.append(score)
        missing_list.append(still_missing)

    recipes_df = recipes_df.copy()
    recipes_df["score"] = scores
    recipes_df["missing_ingredients"] = missing_list
    recipes_df["missing_count"] = [len(x) for x in missing_list]
    recipes_df = recipes_df.sort_values(by=["score", "missing_count"], ascending=[False, True])

    top5 = recipes_df.head(5)

    shopping = []
    for index, row in top5.iterrows():
        for item in row["missing_ingredients"]:
            if item not in shopping:
                shopping.append(item)

    print("Transformation complete.")
    return top5, shopping


def load(top5, shopping):
    print(f"Loading results to {DB_FILE}...")

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_name TEXT,
            score REAL,
            missing_count INTEGER,
            missing_ingredients TEXT,
            timestamp TEXT
        )
    """)

    for index, row in top5.iterrows():
        cur.execute("""
            INSERT INTO recommendations (recipe_name, score, missing_count, missing_ingredients, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (row["recipe_name"], row["score"], row["missing_count"],
              ", ".join(row["missing_ingredients"]), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()
    conn.close()
    print("Load complete.")
    logging.info("results saved to db")


def run_pipeline():
    print("--- Starting Recipe Recommender Pipeline ---")

    fridge_items, recipes_df = extract(FRIDGE_FILE, RECIPE_FILE)
    if fridge_items is None or recipes_df is None:
        print("Pipeline stopped due to extraction error.")
        return

    top5, shopping = transform(fridge_items, recipes_df)
    load(top5, shopping)

    print("\nTop 5 Recommended Recipes:")
    for i, (index, row) in enumerate(top5.iterrows()):
        print(f"\n{i+1}. {row['recipe_name']}")
        print(f"   Match Score : {row['score']:.1f}%")
        print(f"   Missing Items: {row['missing_count']}")
        if row['missing_ingredients']:
            print(f"   Need to buy  : {', '.join(row['missing_ingredients'])}")

    print("\nShopping List:")
    if shopping:
        for item in shopping:
            print(f"  - {item}")
    else:
        print("  Nothing needed!")

    conn = sqlite3.connect(DB_FILE)
    history = pd.read_sql("SELECT * FROM recommendations", conn)
    conn.close()

    freq = history.groupby("recipe_name").agg(
        times_recommended=("recipe_name", "count"),
        avg_score=("score", "mean")
    ).reset_index().sort_values("times_recommended", ascending=False)

    print("\n--- Analysis: Most Recommended Recipes ---")
    print(freq.to_string(index=False))

    print("\n--- Pipeline Finished ---")
    logging.info("pipeline finished")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    run_pipeline()
