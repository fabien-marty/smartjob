import hashlib
import json
import os

# These 2 environment variables are automatically injected by SmartJob
# They are pointing to a local mount of the staging bucket
# input_path and output_path are two unique directories specific
# to this job execution
input_path = os.environ.get("INPUT_PATH")
output_path = os.environ.get("OUTPUT_PATH")

# Let's read the input file: 'my-input-file'
with open(f"{input_path}/my-input-file", "rb") as f:
    content = f.read()

# Compute the hash of the input file
h = hashlib.sha1(content).hexdigest()

# Let's write the hash to the output file
# Note: you can write any file you want in the output directory
#       but the smartjob.json file is a special file that will be
#       automatically parsed by the SmartJob lib and the content will be
#       injected in the Python result object.
with open(f"{output_path}/smartjob.json", "w") as f:
    json.dump({"hash": h}, f)
