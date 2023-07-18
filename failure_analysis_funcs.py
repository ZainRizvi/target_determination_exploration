import re

def get_files_from_traceback(traceback):
    if "Traceback" not in traceback:
        print("Not a traceback")
        return []

    traceback_files_reg = re.compile(r"File \"(.*\.py)\", line")
    files = set()
    traceback_lines = traceback.split("\n")
    for line in traceback_lines:
        match = traceback_files_reg.search(line)
        if not match:
            continue

        file = match.group(1)
        match = re.search(r"(python[^\/]*\/site-packages\/)(.*)", file)
        if match:
            # The stuff after site-packages is the github repo
            file = match.group(2)

        if file.startswith("/opt/conda/envs/py_"):
            continue # These are 3rd party packages

        # remove the workspace prefix, if it exists
        file = file.removeprefix("/var/lib/jenkins/workspace/")

        files.add(file)


    return files

def get_files_from_failure_stack_trace(pp_row):
    # print(f"Row is {pp_row}")
    err_message = pp_row["failure.text"]

    if not isinstance(err_message, str):
        print("Invalid error message")
        return ""

    if "Traceback" not in err_message:
        # print("Not a traceback")
        return ""

    return ",".join(get_files_from_traceback(err_message))

def modified_files_in_stack(pp_row):
    # print(f"Row is {pp_row}")
    modified_files = set()

    if not pp_row["traceback_files"]:
        return None

    traceback_files = pp_row["traceback_files"].split(",")
    modded_files = pp_row["files"].split(",")

    for traceback_file in traceback_files:
        if "test_" in traceback_file:
            continue
        for mod_file in modded_files:
            if traceback_file in mod_file:
                modified_files.add(traceback_file)
                break

    if modified_files:
        return modified_files
    return None
