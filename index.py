from typing import Dict, Tuple
from github import Github, GithubException
from mydb_types import Records, Indexes
from collections import defaultdict
import json
import os
import time
from errors import IndexError, ValidationError

class IndexManager:
    @staticmethod
    def build_index(fields: Tuple[str, ...], data: Records, indexes: Indexes):
        if not fields:
            raise ValidationError("Index fields cannot be empty")
        for field in fields:
            if not isinstance(field, str) or not field.strip():
                raise ValidationError(f"Invalid index field: {field}")
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Index field '{field}' is reserved")
        index = defaultdict(set)
        try:
            for id_, record in data.items():
                if all(field in record for field in fields):
                    composite_key = "|".join(str(record[field]) for field in fields)
                    index[composite_key].add(id_)
            index_key = ",".join(fields)
            indexes[index_key] = dict(index)
            IndexManager.save_index_to_file(indexes[index_key], index_key)
        except Exception as e:
            raise IndexError(f"Failed to build index on {','.join(fields)}: {e}")

    @staticmethod
    def save_index_to_file(index: Dict, index_key: str):
        if not index_key:
            raise ValidationError("Index key cannot be empty")
        index_file = "index_db.json"
        github_token = os.environ.get("GITHUB_TOKEN")
        github_repo = os.environ.get("GITHUB_REPO", "your-username/mydb-streamlit")
        if not github_token:
            raise ValidationError("GITHUB_TOKEN environment variable is required")
        if not re.match(r"^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$", github_repo):
            raise ValidationError("Invalid GitHub repository format. Expected 'username/repo'")
        try:
            existing_index = {}
            g = Github(github_token)
            repo = g.get_repo(github_repo)
            try:
                file = repo.get_contents(index_file, ref="main")
                existing_index = json.loads(file.decoded_content.decode())
            except GithubException as e:
                if e.status != 404:
                    raise IndexError(f"Failed to load existing index from GitHub: {e}")
            existing_index[index_key] = {k: list(v) for k, v in index.items()}
            content = json.dumps(existing_index, indent=2)
            try:
                file = repo.get_contents(index_file, ref="main")
                repo.update_file(
                    index_file,
                    f"Update {index_file} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    content,
                    file.sha,
                    branch="main"
                )
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(
                        index_file,
                        f"Create {index_file} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        content,
                        branch="main"
                    )
                else:
                    raise IndexError(f"GitHub API error: {e}")
        except GithubException as e:
            raise IndexError(f"Failed to save index to GitHub: {e}")
        except Exception as e:
            raise IndexError(f"Unexpected error saving index to GitHub: {e}")
