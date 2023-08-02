import os
import importlib
from script.utils.loggingSetup import setup_logger, log_info, log_error, log_warning

def load_pipelines(pipeline_dir):
    pipelines = []
    log_info(f"Loading pipelines from {pipeline_dir}")
    for filename in os.listdir(pipeline_dir):
        if filename.endswith(".py") and filename != "__init__.py":
            pipeline_name = filename[:-3]  # Remove the ".py" extension
            module_path = f"{pipeline_dir}.{pipeline_name}"
            try:
                module = importlib.import_module(module_path)
                pipelines.append(module)
            except Exception as e:
                print(f"Error loading {pipeline_name}: {e}")
    return pipelines

def execute_pipelines(pipelines):
    for pipeline in pipelines:
        log_info(f"Executing pipeline '{pipeline.__name__}'")
        try:
            pipeline.run()
        except AttributeError:
            log_error(f"Error: pipeline '{pipeline.__name__}' does not have a 'run' function.")
        except Exception as e:
            log_error(f"Error executing pipeline '{pipeline.__name__}': {e}")


if __name__ == "__main__":
    setup_logger()
    pipeline_directory = "pipelines"
    loaded_pipelines = load_pipelines(pipeline_directory)
    execute_pipelines(loaded_pipelines)
