import json
import os
from pathlib import Path
from typing import List, NamedTuple, TextIO, Union
from pandas import DataFrame
import pandas as pd
import rockset
import time
from datetime import datetime, timedelta
import subprocess

def show_all_pandas_data():
    # set pandas to print all rows
    pd.set_option('display.max_rows', None)
    # setup pandas to stop concatenating urls
    pd.set_option('display.max_colwidth', None)
    # setup pandas to print all columns
    pd.set_option('display.max_columns', None)


def pretty_delta(delta: timedelta) -> str:
    def pluralize(number, unit):
        return f"{number} {unit}{'s' if number != 1 else ''}"

    total_seconds = int(delta.total_seconds())

    # Calculate the components of the time delta
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    # Format the time delta as a string
    parts = []
    if days:
        parts.append(pluralize(days, "day"))
    if hours:
        parts.append(pluralize(hours, "hour"))
    if minutes:
        parts.append(pluralize(minutes, "minute"))
    if seconds:
        parts.append(pluralize(seconds, "second"))

    return ', '.join(parts) or "0 seconds"


def print_file_size(file_path_pointer: TextIO):
    file_size = os.fstat(file_path_pointer.fileno()).st_size
    print(f"Cache size: {round(file_size / 1024 / 1024, 1)} MB")

def cache_response_on_disk(
        cache_file_base: str,
        file_suffix_param: str = "",
        folder: Union[str, Path] = "data",
        expiration_fmt: str = '%Y-%m-week_%W', # The cache expires at the end of the week
):
    # Ensure we have a Path object
    if isinstance(folder, str):
        folder = Path(folder)

    def decorator(fn):

        def wrapper(*args, **kwargs):
            file_suffix = ""
            if  file_suffix_param and file_suffix_param in kwargs and kwargs[file_suffix_param]:
                file_suffix = str(kwargs[file_suffix_param])
            elif file_suffix_param:
                file_suffix = file_suffix_param

            expiration_key = datetime.now().strftime(expiration_fmt)

            # filter(None, ...) removes empty Falsey entries from the list
            file_name_base = "_".join(filter(None, [cache_file_base, file_suffix, expiration_key]))
            cache_file = folder / f"{file_name_base}.json"

            if cache_file.is_file():
                # Share how old the cache file is
                cache_creation_time = datetime.fromtimestamp(cache_file.stat().st_ctime)
                print(f"Cache file is {pretty_delta(datetime.now() - cache_creation_time)} old")

                try:
                    with cache_file.open(encoding='utf-8') as cache_fp:
                        cached_data = json.load(cache_fp)
                        print(f"Loaded cached data from {cache_file}")
                        print_file_size(cache_fp)
                        return cached_data
                except Exception as e:
                    print(f"Error loading cache file {cache_file}: {e}")
                    print(f"Falling back to calling function `{fn.__name__}`")

            data = fn(*args, **kwargs)
            with cache_file.open("w", encoding='utf-8') as cache_fp:
                json.dump(data, cache_fp, ensure_ascii=False, indent=1, default=str)
                print_file_size(cache_fp)
            return data
        return wrapper
    return decorator


def get_rockset_client():
    ROCKSET_KEY = os.getenv("ROCKSET_KEY")
    rs = rockset.RocksetClient(
        host=rockset.Regions.usw2a1,
        api_key=ROCKSET_KEY)
    return rs

class RocksetParameter(NamedTuple):
    name: str
    type: str
    value: str

    def to_rockset_parameter(self):
        return rockset.models.QueryParameter(
            name=self.name,
            type=self.type,
            value=self.value,
        )

@cache_response_on_disk(cache_file_base="rockset", file_suffix_param="query_name")
def query_rockset(query: str = "", query_name: str = None, params: list[RocksetParameter] = []):
    """
    Either query or query_file must be specified
    """
    rs = get_rockset_client()

    assert bool(query) != bool(query_name), "Either query or query_file must be specified, but not both"

    if query_name:
        query_file = Path("queries") / f"{query_name}.sql"
        if not query_file.is_file():
            raise ValueError(f"Query file {query_file} does not exist")
        with query_file.open() as fp:
            query = fp.read()


    print(f"Running rockset query: {query_name if query_name else ''}")
    data = []
    page_num = 1
    start_time = time.time()
    for page in rockset.QueryPaginator(
        rs,
        rs.Queries.query(
            sql=rockset.models.QueryRequestSql(
                parameters=[p.to_rockset_parameter() for p in params],
                query=query,
                paginate=True,
                initial_paginate_response_doc_count=10000,
            )
        ),
    ):
        print(f"Received page {page_num}")
        page_num += 1
        data += page

    end_time = time.time()
    print(f"Total rows received: {len(data)}")
    print(f"Time to get data: {round((end_time - start_time)/60.0, 1)} minutes ")
    return data


def build_onto_cache_dict(
        cache_name: str,
        folder: Union[str, Path] = "data",
):
    """
    This decorator results in a dictonary being passed to the function. The function can then
    add to the dictionary, and the dictionary will be saved to disk at the end of the function.

    Useful for caching data that is built up over multiple function calls and doesn't change

    The function being decorated must declare the cache_name as a named parameter.

    Usage:
    @build_onto_cache_dict("my_cache_name")
    def my_function(some_arg, my_cache_name: dict = {}):
    """
    # Ensure we have a Path object
    if isinstance(folder, str):
        folder = Path(folder)

    def decorator(fn):
        def wrapper(*args, **kwargs):
            cache_file = folder / f"cached_{cache_name}.json"


            cached_data = {}
            if cache_file.is_file():
                # Share how old the cache file is
                cache_creation_time = datetime.fromtimestamp(cache_file.stat().st_ctime)
                print(f"Cache file is {pretty_delta(datetime.now() - cache_creation_time)} old")

                try:
                    with cache_file.open(encoding='utf-8') as cache_fp:
                        cached_data = json.load(cache_fp)
                        print(f"Loaded cached data from {cache_file}")
                        print_file_size(cache_fp)
                except Exception as e:
                    print(f"Error loading cache file {cache_file}. Not using a cache: {e}")
            else:
                print(f"Did not find a cache for {cache_name} at {cache_file}")

            use_disk_cache = True
            if cache_name in kwargs:
                print(f"A {cache_name} cache was explicitly passed into the function. Not using the one on disk")
                use_disk_cache = False

            if use_disk_cache:
                kwargs[cache_name] = cached_data

            data = fn(*args, **kwargs)

            if use_disk_cache:
                with cache_file.open("w", encoding='utf-8') as cache_fp:
                    print("Saving data to cache")
                    json.dump(cached_data, cache_fp, ensure_ascii=False, indent=1, default=str)
                    print_file_size(cache_fp)
            return data

        return wrapper
    return decorator

class WorkingDir:
    def __init__(self, directory: str):
        self.directory = directory

    def __enter__(self):
        self.orig_dir = os.getcwd()
        os.chdir(self.directory)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.orig_dir)

def cmd(args: List[str]):
    """
    Run a system command and return the output as a string
    """
    print(f"Running command: {' '.join(args)}")
    result = subprocess.run(args, capture_output=True)
    if result.returncode == 0:
        return result.stdout.decode("utf-8")
    else:
        return result.stderr.decode('utf-8')


INVALID_FILES = "*"

def get_files_for_sha_via_git(sha: str, pr_number: int) -> str:
    print(f"\n\nGetting files for sha: {sha} from PR https://github.com/pytorch/pytorch/pull/{pr_number}")

    merge_base = cmd(['git', 'merge-base', sha, 'main']).strip()
    print(f"Received result: {merge_base}")
    if not merge_base or merge_base.isspace():
        print("No merge base found")
        return INVALID_FILES

    if "Not a valid commit name" in merge_base:
        # Something funky with this PR. Skip it
        print("This one was invalid. Either we haven't pulled this commit yet, or it came from a forked repositry that's not in our git repo")
        print(f"merge_base response = `{merge_base}`")
        files = "*"
        return files

    # Get the files
    files_raw = cmd(['git', 'diff', '--name-only', merge_base, sha])

    # Parse the files
    if not files_raw:
        print("No modified files")
        files = ""
    elif files_raw.isspace() or "Not a valid commit name" in files_raw:
        # Something funky with this PR. Skip it
        print("This one was not a valid commit name 2")
        files = INVALID_FILES
    else:
        files = list(filter(lambda f: f and not f.isspace(), files_raw.split('\n')))
        print(files)
    return files

@build_onto_cache_dict(cache_name="commit_files")
@build_onto_cache_dict(cache_name="invalid_shas")
def get_files_changed(
    df,
    commit_files: dict[str, List[str]] = {},
    invalid_shas: dict[str, None] = {}
) -> List[List[str]]:
    # cache_file = "data/files_changed_cache.json"
    # backup_files = {}
    # with open(cache_file, "r", encoding="utf-8") as file:
    #     backup_files = json.loads(file.read())


    with WorkingDir("/Users/zainr/pytorch"):
        skipped_shas = 0

        pr_shas = df["sha"]
        num_shas = len(pr_shas)
        files_lists = []
        # distinct_shas = set()
        for i, sha in enumerate(pr_shas):
            if i % 100 == 0:
                print(f"Processing sha {i+1}/{num_shas} ({i/num_shas*100:.2f}%)")

            # Find the merge base between this PR and master
            # distinct_shas.add(sha)
            # Get the value of the "pr_number" column that corresponds to this "sha"
            # did we already get it?
            files = commit_files.get(sha)

            if sha in invalid_shas:
                print("We already know we don't have this sha")
                continue

            if files == None or files == "*":
                pr_number = df[df["sha"] == sha]["pr_number"].iloc[0]
                files = get_files_for_sha_via_git(sha, pr_number)
                if files == "*":
                    # Ensure we have this commit locally and try again
                    print(f"Fetching commit {sha}")
                    cmd(['git', 'fetch', 'origin', sha])
                    files = get_files_for_sha_via_git(sha, pr_number)

                commit_files[sha] = files
                if files == "*":
                    invalid_shas[sha] = None
                    print("fetching the commit failed. Skipping it")
                    skipped_shas += 1
            files_lists.append(files)

    return files_lists


def map_df(map_fn, df: pd.DataFrame):
    for index, row in df.iterrows():
        yield map_fn(row)


def print_traceback_files(df_row, stack = False):
    tb_files = df_row["traceback_files"].split(",")
    files = df_row["files"].split(",")

    bullets = "\n - "
    print("\n")
    print(f"PR: http:/github.com/pytorch/pytorch/pull/{int(df_row['pr_number'])}")
    print(f"Test file: {df_row['test_file']}")
    print(f"Test class: {df_row['classname']}")
    print(f"Invoking file: {df_row['invoking_file']}")
    print(f"Sha: {df_row['sha']}")
    print("")
    print(f"tb_files = {bullets}{bullets.join(tb_files)}")
    print(f"files = {bullets}{bullets.join(files)}")
    print(f"mod_files = {bullets}{bullets.join(df_row['mod_in_traceback'])}")
    if stack:
        print(f"failure.text: {df_row['failure.text']}")
    print("\n")