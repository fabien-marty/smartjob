import json
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
EXECUTION_ID = os.environ["EXECUTION_ID"]


def main():
    input1: str | None = None
    if os.path.exists(f"{INPUT_PATH}/input1"):
        with open(f"{INPUT_PATH}/input1") as f:
            input1 = f.read()
    with open(f"{OUTPUT_PATH}/smartjob.json", "w") as f:
        f.write(json.dumps({"input1": input1, "execution_id": EXECUTION_ID}))
    print("DONE")


if __name__ == "__main__":
    main()
