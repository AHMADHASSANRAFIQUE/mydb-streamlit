from typing import Dict, Tuple
from mydb_types import Records, Indexes
from collections import defaultdict
from github import Github
import json
import os

class IndexManager:
    @staticmethod
    def build_index(fields: Tuple[str, ...], data: Records, indexes: Indexes):
        index = defaultdict(set)
        for id_, record in data.items():
            if all(field in record for field in fields):
                composite_key = "|".join(str(record[field]) for field in fields)
                index[composite_key].add(id_)
        index_key = ",".join(fields)
        indexes[index_key] = dict(index)
        IndexManager.save_index_to_file(indexes[index_key], index_key)

    @staticmethod
    def save_index_to_file(index: Dict, index_key: str):
        index_file = "index_db.json"
        try:
            existing_index = {}
            github_token = os.environ.get("GITHUB_TOKEN")
            github_repo = os.environ.get("GITHUB_REPO", "your-username/mydb-streamlit")
            g = Github(github_token)
            repo = g.get_repo(github_repo)
            try:
                file = repo.get_contents(index_file, ref="main")
                existing_index = json.loads(file.decoded_content.decode())
            except:
                pass
            existing_index[index_key] = {k: list(v) for k, v in index.items()}
            try:
                file = repo.get_contents(index_file, ref="main")
                repo.update_file(
                    index_file,
                    f"Update {index_file}",
                    json.dumps(existing_index, indent=2),
                    file.sha,
                    branch="main"
                )
            except:
                repo.create_file(
                    index_file,
                    f"Create {index_file}",
                    json.dumps(existing_index, indent=2),
                    branch="main"
                )
            print(f"Index for {index_key} saved to GitHub")
        except Exception as e:
            print(f"Failed to save index to GitHub: {e}")
