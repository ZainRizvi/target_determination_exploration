import re
import pandas as pd
import re

from pansql import sqldf
from pathlib import Path
from importlib import reload

import utilities
import failure_analysis_funcs as tf

reload(utilities)
utilities.show_all_pandas_data()

query_name = "test_failures"

params = [
    utilities.RocksetParameter(name="num_days_to_cover", type="int", value="100"),
    utilities.RocksetParameter(name="days_back_start", type="int", value="0"),
]

reload(utilities)
data = utilities.query_rockset(query_name=query_name, params=params)
df = pd.json_normalize(data)

# # Useful for making incremental updates to the cache, in case we run into errors
# batch_size = 1000
# for i in range (int(df.size / batch_size)):
#     # Print LOOP in giant ascii art
#     print("  _______ _    _ _______ _____ _______ _____  ______ _____   _____  ______ _")
#     print(" |__   __| |  | |__   __|_   _|__   __|  __ \|  ____|  __ \ / ____|  ____| |")
#     print("    | |  | |__| |  | |    | |    | |  | |__) | |__  | |__) | |  __| |__  | |")
#     print("    | |  |  __  |  | |    | |    | |  |  _  /|  __| |  _  /| | |_ |  __| | |")
#     print("    | |  | |  | |  | |   _| |_   | |  | | \ \| |____| | \ \| |__| | |____|_|")
#     print("    |_|  |_|  |_|  |_|  |_____|  |_|  |_|  \_\______|_|  \_\\_____|______(_)")
#     # ummm...that's not "LOOP", but whatevs


#     print(f"Loop {i} of {df.size / batch_size}")
#     start = i * batch_size
#     end = (i + 1) * batch_size
#     files_changed = utilities.get_files_changed(df[start:end])

reload(utilities)
files_changed = utilities.get_files_changed(df)

# get the row where sha = 00385fa09747c97875fd5858c5e2fa74f420b2c5
df[df["sha"] == "00385fa09747c97875fd5858c5e2fa74f420b2c5"]

df.columns


df["files"] = files_changed
df["files"].describe
df[df["files"]==""] # Are we missinig any data?

# Where failure is not null
df_fails = df[df["failure.text"].notnull()]


# where df has a failure and conclusion is not success
fails = df[(df["failure.text"].notnull()) & (df["conclusion"] != "success")]

# These seem to have been falky failures. We should ignore them
weirdos = df_fails[df_fails["conclusion"] == "success"]
# Get first row
weirdos.iloc[0]

# DO NEXT:
# - Map from the actaul tests that was modified in the stack to the test that failed. Prioritize that first
# - Map from the other files in the stack to the test files that failed. Run them next
# - Add monitoring to see how often this reordering happens

reload(tf)
fails.loc[:,"traceback_files"] = list(utilities.map_df(tf.get_files_from_failure_stack_trace, fails))

reload(tf)
fails.loc[:,"mod_in_traceback"] = list(utilities.map_df(tf.modified_files_in_stack, fails))

reload(tf)
tf.are_modified_files_in_stack(fails.iloc[12])

reload(utilities)
# List just the traceback_files and files columns where traceback_files is not empty
utilities.print_traceback_files(fails[fails["traceback_files"] != ""].iloc[12])

# Get rows with distinct values for "traceback_files, files, and pr_number"
dedupefails = fails.drop_duplicates(subset=["traceback_files", "files", "pr_number"])
utilities.print_traceback_files(dedupefails[dedupefails["traceback_files"] != ""].iloc[12])

# traceback_files that are empty
fails[fails["traceback_files"] == ""].size

# List where mod_in_traceback is true
fails[fails["mod_in_traceback"] == True].size

mod_fail = fails[fails["mod_in_traceback"] == True].drop_duplicates(subset=["traceback_files", "files", "pr_number"])
mod_fail.size
utilities.print_traceback_files(mod_fail.iloc[12])


# Return the ["traceback_files", "files"] columns of the first row where mod_in_traceback is true
fails[fails["mod_in_traceback"] == True][["traceback_files", "files"]].iloc[3]

# Fiew the traceback files of the first row
fails.iloc[0]["traceback_files"]

df["no_cpp"] = list(utilities.map_df(no_cpp_files_touched, df))

# First row of fails


len(df[(df["no_cpp"] == True)])
df[(df["no_cpp"] == False)] # & (pp["files"] != "*")]
df[df["files"] != "*"]

percent_prs_with_cpp = len(df[(df["no_cpp"] == False)]) / len(df)
# Print number of PRs with cpp files  rounded to 2 decimal places
print(f"PRs with cpp files: {percent_prs_with_cpp * 100:.2f}%")

df["no_gpu"] = list(utilities.map_df(skip_gpu_test, df))
df["no_gpu"] = False # what happens if we always run gpu tests?

df["no_gpu"].sum()

