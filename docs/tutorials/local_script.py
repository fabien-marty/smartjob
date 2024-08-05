import os

execution_id = os.environ.get("EXECUTION_ID")

print("Hello world!")
print("I'm going to be executed in a (remote) Cloud Run Job")
print(f"My unique execution id is {execution_id}")
