# Recipe Recommender Pipeline
# Assignment 38 - Data Pipeline
# submitted by: [Your Name]
# date: april 2025

# importing all the libraries i need
import json
import pandas as pd
import sqlite3
import re
import ast
import logging
from datetime import datetime


# setting up logging so teacher can see whats happening
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -------------------------------------------
# STEP 1 - LOAD THE DATA (Data Ingestion)
# -------------------------------------------

# this function reads the fridge json file and returns ingredients
def load_fridge_items(filepath):
    try:
        f = open(filepath, 'r')
        data = json.load(f)
        f.close()
        ingredients = data["ingredients"]
        print("fridge items loaded successfully")
        print("ingredients found:", ingredients)
        logging.info("fridge file loaded ok")
        return ingredients
    except Exception as e:
        print("ERROR: could not load fridge file")
        print(e)
        logging.error("fridge load failed: " + str(e))
        return []


# this function reads the recipe csv file
def load_recipes(filepath):
    try:
        df = pd.read_csv(filepath)
        print("recipes loaded successfully")
        print("total recipes:", len(df))
        logging.info("recipe file loaded ok")
        return df
    except Exception as e:
        print("ERROR: could not load recipe file")
        print(e)
        logging.error("recipe load failed: " + str(e))
        return pd.DataFrame()  # return empty dataframe if error


# -------------------------------------------
# STEP 2 - PROCESS / NORMALIZE INGREDIENTS
# -------------------------------------------

# dictionary for synonyms (words that mean the same thing)
synonyms_dict = {
    "tomatoes": "tomato",
    "potatoes": "potato",
    "onions": "onion",
    "eggs": "egg",
    "chilies": "chili",
    "chillies": "chili",
    "peppers": "pepper",
}

# dictionary for substitutions (similar ingredients)
substitutions = {
    "butter": "margarine",
    "cream": "milk",
    "lemon": "lime",
}


# this function cleans up an ingredient name
def normalize_ingredient(ingredient):
    # make lowercase first
    ingredient = ingredient.lower()

    # remove extra spaces
    ingredient = ingredient.strip()

    # remove numbers and units like 200g, 2 cups etc
    ingredient = re.sub(r'\d+\s*(g|kg|ml|l|cups?|tbsp|tsp|oz|lb|grams?|litres?|liters?)\b', '', ingredient)

    # remove just numbers too
    ingredient = re.sub(r'\d+', '', ingredient)

    # strip again after removing numbers
    ingredient = ingredient.strip()

    # check synonyms
    if ingredient in synonyms_dict:
        ingredient = synonyms_dict[ingredient]

    return ingredient


# apply normalize to a whole list
def process_ingredients(ingredients_list):
    cleaned = []
    for item in ingredients_list:
        cleaned_item = normalize_ingredient(item)
        if cleaned_item != "":  # dont add empty strings
            cleaned.append(cleaned_item)
    return cleaned


# -------------------------------------------
# STEP 3 - MATCHING / SCORING
# -------------------------------------------

# this function checks how many ingredients match
# and returns a score out of 100
def calculate_match_score(fridge_items, recipe_ingredients):
    fridge_set = set(fridge_items)
    recipe_set = set(recipe_ingredients)

    # find ingredients that match
    matched_ingredients = fridge_set.intersection(recipe_set)

    # find what is missing
    missing_ingredients = recipe_set - fridge_set

    # also check substitutions for missing items
    # if we have a substitute ingredient in fridge then dont count as missing
    still_missing = []
    for missing in missing_ingredients:
        found_substitute = False
        for orig, sub in substitutions.items():
            if missing == orig and sub in fridge_set:
                found_substitute = True
            if missing == sub and orig in fridge_set:
                found_substitute = True
        if not found_substitute:
            still_missing.append(missing)

    # calculate completion percentage
    if len(recipe_set) == 0:
        score = 0
    else:
        # formula from assignment: matched / total * 100
        score = (len(matched_ingredients) / len(recipe_set)) * 100

    return score, still_missing


# -------------------------------------------
# STEP 4 - FILTERING
# -------------------------------------------

# filter recipes by diet type
def filter_by_diet(df, diet_type):
    if diet_type is None:
        return df  # no filter needed

    if "diet" not in df.columns:
        print("WARNING: no diet column in recipe file, skipping filter")
        return df

    # filter the dataframe
    filtered = df[df["diet"] == diet_type]
    print(f"recipes after {diet_type} filter:", len(filtered))
    return filtered


# -------------------------------------------
# STEP 5 - RANKING RECIPES
# -------------------------------------------

def rank_recipes(df, fridge_items):
    # lists to store scores
    all_scores = []
    all_missing = []

    # go through each recipe one by one
    for index, row in df.iterrows():
        ing = row["ingredients"]

        # convert string to list (its stored as string in csv)
        try:
            ing_list = ast.literal_eval(ing)
        except:
            ing_list = []  # if error just use empty list

        # normalize the recipe ingredients too
        ing_list = process_ingredients(ing_list)

        # get score and missing for this recipe
        score, missing = calculate_match_score(fridge_items, ing_list)

        all_scores.append(score)
        all_missing.append(missing)

    # add new columns to dataframe
    df = df.copy()
    df["score"] = all_scores
    df["missing_ingredients"] = all_missing
    df["missing_count"] = [len(m) for m in all_missing]  # count of missing items

    # sort: highest score first, then least missing
    df = df.sort_values(by=["score", "missing_count"], ascending=[False, True])

    return df


# -------------------------------------------
# STEP 6 - GENERATE SHOPPING LIST
# -------------------------------------------

def make_shopping_list(top_recipes_df):
    shopping = []

    for index, row in top_recipes_df.iterrows():
        for item in row["missing_ingredients"]:
            if item not in shopping:  # dont add duplicates
                shopping.append(item)

    return shopping


# -------------------------------------------
# STEP 7 - STORE IN DATABASE
# -------------------------------------------

def save_to_database(top_recipes_df):
    # connect to sqlite database (creates file if not exists)
    connection = sqlite3.connect("recipes.db")
    cursor = connection.cursor()

    # create table if it doesnt exist already
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_name TEXT,
            score REAL,
            missing_count INTEGER,
            missing_ingredients TEXT,
            timestamp TEXT
        )
    """)

    # insert each recommended recipe
    for index, row in top_recipes_df.iterrows():
        missing_str = ", ".join(row["missing_ingredients"])
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO recommendations (recipe_name, score, missing_count, missing_ingredients, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (row["recipe_name"], row["score"], row["missing_count"], missing_str, time_now))

    connection.commit()
    connection.close()
    print("results saved to database!")
    logging.info("saved to database successfully")


# -------------------------------------------
# STEP 8 - ANALYSIS FUNCTION
# -------------------------------------------

# this function reads the db and finds most recommended recipes
def show_analysis():
    connection = sqlite3.connect("recipes.db")

    # get all recommendations from history
    df = pd.read_sql("SELECT * FROM recommendations", connection)
    connection.close()

    if df.empty:
        print("no history found in database")
        return

    # count how many times each recipe was recommended
    frequency = df.groupby("recipe_name").agg(
        times_recommended=("recipe_name", "count"),
        avg_score=("score", "mean")
    ).reset_index()

    frequency = frequency.sort_values("times_recommended", ascending=False)

    print("\n=== Analysis: Most Recommended Recipes ===")
    print(frequency.to_string(index=False))


# -------------------------------------------
# STEP 9 - VALIDATION
# -------------------------------------------

def validate_inputs(fridge_items, df):
    # check fridge items
    if fridge_items is None or len(fridge_items) == 0:
        raise ValueError("ERROR: fridge items list is empty! check your json file")

    # check recipe dataframe
    if df is None or df.empty:
        raise ValueError("ERROR: recipe dataframe is empty! check your csv file")

    # check required columns exist
    required_cols = ["recipe_name", "ingredients"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"ERROR: column '{col}' not found in recipe file")

    # check for missing values
    if df["recipe_name"].isnull().any():
        print("WARNING: some recipes have missing names")

    if df["ingredients"].isnull().any():
        print("WARNING: some recipes have missing ingredients")

    print("validation passed!")


# -------------------------------------------
# MAIN PIPELINE FUNCTION
# -------------------------------------------

def run_pipeline(fridge_path, recipe_path, diet_filter=None):

    print("=" * 50)
    print("RECIPE RECOMMENDER PIPELINE STARTED")
    print("=" * 50)

    # step 1: load data
    print("\n[Step 1] Loading data...")
    fridge_items = load_fridge_items(fridge_path)
    recipes_df = load_recipes(recipe_path)

    # step 2: validate
    print("\n[Step 2] Validating data...")
    validate_inputs(fridge_items, recipes_df)

    # step 3: normalize fridge ingredients
    print("\n[Step 3] Processing fridge ingredients...")
    fridge_items = process_ingredients(fridge_items)
    print("normalized fridge:", fridge_items)

    # step 4: filter by diet
    print("\n[Step 4] Filtering recipes...")
    recipes_df = filter_by_diet(recipes_df, diet_filter)

    # step 5: rank
    print("\n[Step 5] Ranking recipes...")
    ranked_df = rank_recipes(recipes_df, fridge_items)

    # get top 5 recipes
    top5 = ranked_df.head(5)

    # step 6: shopping list
    print("\n[Step 6] Generating shopping list...")
    shopping_list = make_shopping_list(top5)

    # step 7: save to db
    print("\n[Step 7] Saving to database...")
    save_to_database(top5)

    # step 8: show results
    print("\n" + "=" * 50)
    print("TOP 5 RECOMMENDED RECIPES:")
    print("=" * 50)
    for i, (index, row) in enumerate(top5.iterrows()):
        print(f"\n{i+1}. {row['recipe_name']}")
        print(f"   Match Score: {row['score']:.1f}%")
        print(f"   Missing Items: {row['missing_count']}")
        if row['missing_ingredients']:
            print(f"   Need to buy: {', '.join(row['missing_ingredients'])}")

    print("\n" + "=" * 50)
    print("SHOPPING LIST:")
    print("=" * 50)
    if shopping_list:
        for item in shopping_list:
            print(f"  - {item}")
    else:
        print("  Nothing! You have everything you need :)")

    # step 8 (analysis - optional)
    show_analysis()

    print("\nPipeline finished successfully!")
    logging.info("pipeline ran successfully")


# this runs the pipeline when you run this file
if __name__ == "__main__":
    # change these paths if your files are in a different location
    run_pipeline(
        fridge_path="Fridge_Items.json",
        recipe_path="recipes.csv",
        diet_filter=None   # change to "vegetarian" or "vegan" to filter
    )


# ===================================
# SAMPLE DATA (for testing)
# ===================================

# Fridge_Items.json:
# {
#   "ingredients": ["tomato", "onion", "milk", "salt", "egg"]
# }

# recipes.csv:
# recipe_id,recipe_name,ingredients,diet
# 1,Tomato Soup,"['tomato','salt','water']",vegetarian
# 2,Masala Omelette,"['egg','onion','salt']",vegetarian
# 3,Pasta,"['pasta','tomato','cheese']",vegetarian
# 4,Aloo Sabzi,"['potato','onion','salt','oil']",vegan