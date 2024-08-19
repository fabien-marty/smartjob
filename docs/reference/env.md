# Environnement variables (inside the job container)

These env variables are automatically injected inside the container environment:

- `EXECUTION_ID`: a unique hexa identifier (per execution)
- `INPUT_PATH`: local path (inside the container tree) where you can find input files (it's an already created directory dedicated to this execution)
- `OUTPUT_PATH`: local path (inside the container tree) where you can write output files (it's an already created directory dedicated to this execution)

In `OUTPUT_PATH`, you can create a `smartjob.json` file that will be automatically donwloaded/decoded
inside the field `json_output` in the [smartjob.ExecutionResult][] object.