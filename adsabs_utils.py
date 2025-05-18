
import glob
import pandas as pd
import glob
import json
from collections import Counter
import re
import yaml
import ast
import country_converter as coco
import os


def combine_csv_data():

    csv_path = 'ads_data'
    csv_files = glob.glob(csv_path + "/*.csv")

    # Function to extract the keyword between first and second underscore
    def extract_keyword(filepath):
        filename = filepath.split("\\")[-1]  # or use os.path.basename(filepath)
        match = re.match(r"ads_([^_]+)_", filename)
        return match.group(1) if match else "UNKNOWN"

    # Read files and add 'my_keyword' column
    dfs = []
    for file in csv_files:
        keyword = extract_keyword(file)
        df = pd.read_csv(file)
        print (f'Dataframe with keyword: {keyword} - {len(df)} articles')
        df["my_keyword"] = keyword
        dfs.append(df)

    # Optionally, concatenate all into one DataFrame
    all_data = pd.concat(dfs, ignore_index=True)
    print (f'{len(all_data)} retrived articles')


    # Define a custom aggregation function
    agg_dict = {
        "my_keyword": lambda x: ",".join(sorted(set(x))),
    }

    # Add 'last' for all other columns except 'bibcode' and 'my_keyword'
    for col in all_data.columns:
        if col not in ["bibcode", "my_keyword"]:
            agg_dict[col] = "last"

    # Group by 'bibcode' and apply the aggregation
    grouped = (
        all_data.groupby("bibcode", dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    print (f'{len(grouped)} unique articles')

    no_author = grouped[grouped['author'] != grouped['author']]
    print (f'{len(no_author)} articles without author')

    with_author_grouped = grouped[grouped['author'] == grouped['author']]
    print (f'{len(with_author_grouped)} unique articles with author')


    with_author_grouped.to_parquet('ads_data/all_data_unique.parquet', index=False)
    print (f'Saved in ads_data/all_data_unique.parquet')

    return with_author_grouped



def combine_csv_data_from_list(filenames, csv_path='ads_data'):
    """
    Combines selected ADS CSV files from a folder and tags each entry with a keyword
    extracted from the filename.

    Parameters:
        filenames (list of str): List of filenames (not full paths) to read from csv_path.
        csv_path (str): Folder where the CSVs are located (default: 'ads_data').

    Returns:
        pd.DataFrame: Combined and deduplicated DataFrame with 'my_keyword' column.
    """

    # Extract keyword from filename
    def extract_keyword(filename):
        match = re.match(r"ads_([^_]+)_", filename)
        return match.group(1) if match else "UNKNOWN"

    dfs = []
    for fname in filenames:
        full_path = os.path.join(csv_path, fname)
        keyword = extract_keyword(fname)
        df = pd.read_csv(full_path)
        print(f"Dataframe with keyword: {keyword} - {len(df)} articles")
        df["my_keyword"] = keyword
        dfs.append(df)

    all_data = pd.concat(dfs, ignore_index=True)
    print(f"{len(all_data)} retrieved articles")

    # Aggregation logic
    agg_dict = {
        "my_keyword": lambda x: ",".join(sorted(set(x))),
    }
    for col in all_data.columns:
        if col not in ["bibcode", "my_keyword"]:
            agg_dict[col] = "last"

    grouped = all_data.groupby("bibcode", dropna=False).agg(agg_dict).reset_index()
    print(f"{len(grouped)} unique articles")

    no_author = grouped[grouped['author'] != grouped['author']]
    print(f"{len(no_author)} articles without author")

    with_author_grouped = grouped[grouped['author'] == grouped['author']]
    print(f"{len(with_author_grouped)} unique articles with author")

    output_path = os.path.join(csv_path, 'astronomy_data_unique.parquet')
    with_author_grouped.to_parquet(output_path, index=False)
    print(f"Saved in {output_path}")

    return with_author_grouped




class AdsAffCountry:
    def __init__(self, all_data, manual_country_map, us_states):
        self.all_data = all_data
        self.manual_country_map = manual_country_map
        self.us_states = us_states

    # Step 1: Extract country from raw aff string
    def extract_country_strict(self, aff_string):
        try:
            aff_list = ast.literal_eval(aff_string)
            if not aff_list:
                return None
            first_aff = aff_list[0]
            country = first_aff.split(",")[-1].strip()
            return country
        except:
            return None

    # Step 2: Interpret suffixes like state abbreviations, zip codes, etc.
    def clean_country_suffix(self, suffix):
        if not isinstance(suffix, str):
            return None
        suffix = suffix.strip().upper()
        # Remove punctuation and zip codes
        suffix_clean = re.sub(r"[^A-Z\s]", "", suffix)
        # Check for dash or empty
        if suffix_clean in {"", "-", "NAN"}:
            return None
        # Check for US state abbreviations
        if suffix_clean in self.us_states:
            return "USA"
        # Match ZIP code formats (e.g. AZ 85721 or CT 06520)
        if re.match(r"[A-Z]{2}\s*\d{5}", suffix):
            return "USA"
        # Handle common cases like "USA", "U.S.", etc.
        if "USA" in suffix_clean or "UNITED STATES" in suffix_clean:
            return "USA"
        # Otherwise return cleaned name
        return suffix_clean.title()

    def clean_country_column(self, df, input_col, manual_country_map=None, final_col="final_country"):
        """
        Cleans and standardizes a country column in a DataFrame using country_converter and a manual mapping.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            input_col (str): Name of the column containing raw country names.
            manual_country_map (dict): Optional manual mapping for ambiguous or incorrect country names.
            final_col (str): Name of the output column with cleaned country names.

        Returns:
            pd.DataFrame: A copy of the DataFrame with the added/updated `final_col`.
        """
        df = df.copy()
        cc = coco.CountryConverter()

        # Valid short-form country names
        valid_countries = set(cc.data['name_short'].unique())
        valid_countries.update(["USA", "Turkey"])

        # Manual map default
        if manual_country_map is None:
            manual_country_map = {}

        # Step 1: Normalize using country_converter
        def normalize_country_name(name):
            try:
                return cc.convert(names=name, to="short_name", not_found=None)
            except:
                return None

        df["normalized_country"] = df[input_col].apply(normalize_country_name)

        # Step 2: Handle lists (just in case)
        df["normalized_country"] = df["normalized_country"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )

        # Step 3: Manual fix map
        def apply_manual_fix(x):
            if isinstance(x, str):
                #return manual_country_map.get(x.strip().title(), x.strip().title())
                key = x.strip()
                return manual_country_map.get(key, key)
            return x

        df["manual_fixed_country"] = df["normalized_country"].apply(apply_manual_fix)

        # Step 4: Final column — vectorized fallback logic
        # Start with original input
        df[final_col] = df[input_col]

        # Use manual fix if valid
        mask_manual = df["manual_fixed_country"].isin(valid_countries)
        df.loc[mask_manual, final_col] = df.loc[mask_manual, "manual_fixed_country"]

        # If still not valid, try normalized version
        mask_normalized = (~df[final_col].isin(valid_countries)) & df["normalized_country"].isin(valid_countries)
        df.loc[mask_normalized, final_col] = df.loc[mask_normalized, "normalized_country"]

        # Step 5: Final validation — only keep valid countries
        df[f'{final_col}_valid'] = df[final_col].apply(
            lambda x: x if x in valid_countries else None
        )
        df['first_author_aff_country_1_valid'] = df['first_author_aff_country_1'].apply(
        lambda x: x if x in valid_countries else None
        )

        df['first_author_aff_country_2_valid'] = df['first_author_aff_country_2'].apply(
            lambda x: x if x in valid_countries else None
        )

        # Clean up
        #df.drop(columns=["normalized_country", "manual_fixed_country"], inplace=True)

        return df


    def final_old(self):
        
        self.all_data["first_author_aff_country_1"] = self.all_data["aff"].apply(self.extract_country_strict)
        self.all_data["first_author_aff_country_2"] = self.all_data["first_author_aff_country_1"].apply(self.clean_country_suffix)
        cleaned_df = self.clean_country_column(
                self.all_data,
                input_col="first_author_aff_country_2",
                manual_country_map=self.manual_country_map,
                final_col="first_author_aff_country_final"
                )
        cleaned_df.to_parquet('ads_data/all_data_aff.parquet', index=False)
        print (f'Saved data to ads_data/all_data_aff.parquet')
        return self.all_data

    def final(self, output_file='ads_data/all_data_aff.parquet'):
        self.all_data["first_author_aff_country_1"] = self.all_data["aff"].apply(self.extract_country_strict)
        self.all_data["first_author_aff_country_2"] = self.all_data["first_author_aff_country_1"].apply(self.clean_country_suffix)
        cleaned_df = self.clean_country_column(
            self.all_data,
            input_col="first_author_aff_country_2",
            manual_country_map=self.manual_country_map,
            final_col="first_author_aff_country_final"
        )
        cleaned_df.to_parquet(output_file, index=False)
        print(f'Saved data to {output_file}')
        return self.all_data



def aff_country_old():

    with open("ads_data/manual_country_map.yaml", "r", encoding="utf-8") as f:
            manual_country_map = yaml.safe_load(f)

    us_states = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    }

    all_data = pd.read_parquet('ads_data/all_data_unique.parquet')

    ads = AdsAffCountry(all_data, manual_country_map, us_states)
    ads.final()
    print ('Done')


def aff_country(input_file=None, output_file=None):
    import yaml
    import pandas as pd

    with open("ads_data/manual_country_map.yaml", "r", encoding="utf-8") as f:
        manual_country_map = yaml.safe_load(f)

    us_states = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
        "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
        "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
        "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    }

    all_data = pd.read_parquet(input_file)
    ads = AdsAffCountry(all_data, manual_country_map, us_states)
    ads.final(output_file=output_file)
    print('Done')



import os
import requests
import pycountry

class CountryFlagDownloader:
    COUNTRY_ALIASES = {
        'USA': 'United States',
        'UK': 'United Kingdom',
        'Turkey': 'Türkiye',
        'South Korea': 'Korea, Republic of',
        'North Korea': "Korea, Democratic People's Republic of",
        'Russia': 'Russian Federation',
        'Iran': 'Iran, Islamic Republic of',
        'Venezuela': 'Venezuela, Bolivarian Republic of',
        'Syria': 'Syrian Arab Republic',
        'Macedonia': 'North Macedonia',
        'Taiwan': 'Taiwan, Province of China',
        'Vietnam': 'Viet Nam', "European Union": "eu",
    }

    def __init__(self, folder="flags", size=(64, 48)):
        self.folder = folder
        self.size = size  # (width, height)
        os.makedirs(self.folder, exist_ok=True)

    def get_country_code(self, country_name):
        name = self.COUNTRY_ALIASES.get(country_name.strip(), country_name.strip())
        try:
            country = pycountry.countries.lookup(name)
            return country.alpha_2.lower()
        except LookupError:
            print(f"[!] Could not find ISO code for: {country_name}")
            return None

    def download_flag(self, code):
        width, height = self.size
        url = f"https://flagcdn.com/{width}x{height}/{code}.png"
        path = os.path.join(self.folder, f"{code}.png")

        if os.path.exists(path):
            print(f"✅ Already exists: {code}.png")
            return

        response = requests.get(url)
        if response.status_code == 200:
            with open(path, "wb") as f:
                f.write(response.content)
            print(f"✅ Downloaded: {code}.png")
        else:
            print(f"❌ Failed to download {code.upper()}: HTTP {response.status_code}")

    def download_flags(self, countries):
        """
        Takes a list of country names and downloads the corresponding flags.
        """
        for country in countries:
            code = self.get_country_code(country)
            if code:
                self.download_flag(code)

    def download_flags_from_df(self, df, country_col, top_n=None):
        """
        Extracts countries from a DataFrame and downloads flags.
        If top_n is given, it limits to the top N most frequent countries.
        """
        counts = df[country_col].value_counts()
        country_list = counts.head(top_n).index if top_n else counts.index
        self.download_flags(country_list)

# Create an instance
#flagger = CountryFlagDownloader(folder="flags", size=(64, 48))
# Download for a custom list
#flagger.download_flags(["USA", "China", "Germany", "Brazil", "Taiwan"])
# Or download from a DataFrame
#flagger.download_flags_from_df(ads_data_aff, country_col="first_author_aff_country_final_valid", top_n=200)





def extract_primary_journal(df, bibstem_col='bibstem', output_col='journal'):
    """
    Extracts the primary journal name from a stringified list in the `bibstem` column.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        bibstem_col (str): Name of the column containing stringified lists of journal names.
        output_col (str): Name of the output column to store the primary journal.
        
    Returns:
        pd.DataFrame: A copy of the DataFrame with the new column added.
    """
    df = df.copy()
    df[output_col] = df[bibstem_col].apply(
        lambda x: ast.literal_eval(x)[0] if isinstance(x, str) and x.startswith('[') else x
    )
    return df

#ads_data_aff = extract_primary_journal(ads_data_aff)


def group_eu_countries(df, country_col='first_author_aff_country_final_valid', output_col='country_grouped'):
    """
    Groups EU countries under 'European Union' in the given DataFrame column.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        country_col (str): Column name containing country names.
        output_col (str): Column name to store the grouped country output.

    Returns:
        pd.DataFrame: A copy of the DataFrame with the grouped country column added.
    """
    eu_countries = {
        "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Czech Republic",
        "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
        "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands",
        "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
    }

    df = df.copy()
    df[output_col] = df[country_col].apply(
        lambda x: "European Union" if x in eu_countries else x
    )
    return df
#ads_data_aff = group_eu_countries(ads_data_aff)


import pycountry

def add_country_code_column(df, input_col='first_author_aff_country_final_valid', output_col='country_code'):
    """
    Adds a new column with 2-letter ISO country codes based on a country name column.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        input_col (str): Column with full country names.
        output_col (str): Name of the new column to store 2-letter country codes.

    Returns:
        pd.DataFrame: Copy of the original DataFrame with the new column added.
    """
    manual_map = {
        "Türkiye": "tr", "Turkey": "tr",
        "Russian Federation": "ru", "Russia": "ru",
        "USA": "us", "United States": "us",
        "UK": "gb", "United Kingdom": "gb",
        "South Korea": "kr", "Korea, Republic of": "kr",
        "North Korea": "kp", "Korea, Democratic People's Republic of": "kp",
        "Iran": "ir", "Iran, Islamic Republic of": "ir",
        "Taiwan": "tw", "Taiwan, Province of China": "tw",
        "European Union": "eu",
    }

    def get_code(name):
        if not isinstance(name, str):
            return None
        name = name.strip()
        if name in manual_map:
            return manual_map[name]
        try:
            return pycountry.countries.lookup(name).alpha_2.lower()
        except LookupError:
            print(f"[!] Could not find ISO code for: {name}")
            return None

    df = df.copy()
    df[output_col] = df[input_col].apply(get_code)
    return df
#ads_data_aff = add_country_code_column(ads_data_aff)


import pandas as pd
import difflib

class SJRMatcher:
    def __init__(self, sjr_path, ads_path):
        self.sjr_path = sjr_path
        self.ads_path = ads_path
        self.sjr_df = pd.read_csv(sjr_path)
        self.ads_data_aff = pd.read_parquet(ads_path)

    def fuzzy_match_journals(self, journal_list, sjr_titles, cutoff=0.9):
        matches = []
        for journal in journal_list:
            match = difflib.get_close_matches(journal, sjr_titles, n=1, cutoff=cutoff)
            if match:
                score = difflib.SequenceMatcher(None, journal, match[0]).ratio()
                matches.append({
                    'original': journal,
                    'matched_sjr_title': match[0],
                    'similarity': score
                })
        return pd.DataFrame(matches)

    def run(self, cutoff=0.9, output_path=None):
        unique_journals = self.ads_data_aff['pub'].dropna().unique().tolist()
        sjr_titles = self.sjr_df['Title'].dropna().unique().tolist()

        fuzzy_matches_df = self.fuzzy_match_journals(unique_journals, sjr_titles, cutoff=cutoff)

        sjr_extended = fuzzy_matches_df.merge(
            self.sjr_df,
            left_on='matched_sjr_title',
            right_on='Title',
            how='left'
        )

        sjr_extended['source'] = 'fuzzy_match'
        sjr_extended['matched_sjr_title_full'] = sjr_extended['Title']
        sjr_extended['Title'] = sjr_extended['original']

        sjr_df_clean = self.sjr_df.copy()
        sjr_df_clean['original'] = sjr_df_clean['Title']
        sjr_df_clean['matched_sjr_title'] = None
        sjr_df_clean['matched_sjr_title_full'] = sjr_df_clean['Title']
        sjr_df_clean['similarity'] = None
        sjr_df_clean['source'] = 'sjr_original'

        common_cols = ['Title', 'original', 'matched_sjr_title', 'matched_sjr_title_full', 'similarity', 'source']
        metadata_cols = [c for c in self.sjr_df.columns if c != 'Title']
        final_cols = common_cols + metadata_cols

        sjr_df_clean = sjr_df_clean[final_cols]
        sjr_extended = sjr_extended[final_cols]

        sjr_df_updated = pd.concat([sjr_df_clean, sjr_extended], ignore_index=True)
        sjr_df_updated = sjr_df_updated.drop_duplicates(subset='Title', keep='first')

        final_cols_to_save = ['Title', 'matched_sjr_title_full', 'similarity', 'source',
                              'SJR Best Quartile', 'Last_seen_year']
        sjr_df_final = sjr_df_updated[final_cols_to_save]

        if output_path:
            sjr_df_final.to_csv(output_path, index=False)

        return sjr_df_final
# Example usage:
#matcher = SJRMatcher(
#    sjr_path="different_data/scimagojr_combined_2001_2023.csv",
#    ads_path="ads_data/all_data_aff.parquet"
#)
#sjr_df_updated_final_1 = matcher.run(cutoff=0.9, output_path="different_data/scimagojr_combined_fuzzy_extended_final.csv")


def classify_grouped_keywords(kw):
    if pd.isna(kw):
        return None
    if "Astrobiology" in kw:
        return "Astrobiology"
    elif "Cosmology" in kw:
        return "Cosmology"
    elif kw.strip() == "AI":
        return "AI"
    else:
        return "Astronomy"

# Example usage:
#ads_data_aff['grouped_keywords'] = ads_data_aff['my_keyword'].apply(classify_grouped_keywords)
