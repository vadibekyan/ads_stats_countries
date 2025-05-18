import requests
import pandas as pd
import json
import time

class ADSQuery:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = "https://api.adsabs.harvard.edu/v1/search/query"
        self.headers = {"Authorization": f"Bearer {self.api_token}"}
        self.requests_made = 0
        self.remaining_requests = None
        self.limit_requests = None
        self.reset_time = None

    def _update_rate_limit_info(self, response):
        self.remaining_requests = response.headers.get("X-RateLimit-Remaining")
        self.limit_requests = response.headers.get("X-RateLimit-Limit")
        self.reset_time = response.headers.get("X-RateLimit-Reset")

    def query_articles(self, start_year, end_year, keyword):
        all_results = []

        for year in range(start_year, end_year + 1):
            params = {
                "q": f"year:{year} {keyword} doctype:article",
                "fl": "abstract,aff,author,bibcode,bibstem,citation_count,date,database,doi,doctype,"
                      "first_author,keyword,pub,pubdate,read_count,title,year,arxiv_class,property,reference",
                "fq": "(database:astronomy) property:refereed",
                "rows": 1000,
                "start": 0
            }

            while True:
                response = requests.get(self.base_url, headers=self.headers, params=params)
                self.requests_made += 1
                self._update_rate_limit_info(response)

                if response.status_code != 200:
                    print(f"❌ Error querying ADS API for year {year}: {response.status_code}")
                    break

                data = response.json().get("response", {}).get("docs", [])
                if not data:
                    break

                all_results.extend(data)
                params["start"] += params["rows"]

        df = pd.DataFrame(all_results)
        df = df[df['doi'].notnull()]
        return df

    def save_to_csv(self, df, filename):
        try:
            df.to_csv(filename, index=False)
            print(f"💾 Saved CSV to {filename}")
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")

    def save_to_json(self, df, filename):
        try:
            df.to_json(filename, orient="records", indent=4)
            print(f"💾 Saved JSON to {filename}")
        except Exception as e:
            print(f"❌ Error saving JSON: {e}")

    def query_articles_and_format_refs(self, start_year, end_year, keyword, output_prefix):
        """
        Query ADS for articles and format the reference column.
        Saves the result as CSV and JSON with formatted references.
        """
        print(f"🔍 Querying articles from {start_year} to {end_year} with keyword '{keyword}'...")
        df = self.query_articles(start_year, end_year, keyword)

        if 'reference' in df.columns:
            df["reference"] = df["reference"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and x else "None"
            )

        csv_name = f"{output_prefix}_with_formatted_refs.csv"
        json_name = f"{output_prefix}_with_formatted_refs.json"

        self.save_to_csv(df, csv_name)
        self.save_to_json(df, json_name)

        print(f"✅ Processed {len(df)} articles. Saved with formatted references.")
        return df


# --- USAGE ---
api_token = "token"
keyword = ""
start_year = 2000
end_year = 2007
output_prefix = f"ads_{keyword}_{start_year}_{end_year}"
ads = ADSQuery(api_token)

df = ads.query_articles_and_format_refs(start_year, end_year, keyword, output_prefix)
