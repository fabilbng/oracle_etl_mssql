from script.utils.loggingSetup import setup_logger, log_info, log_error, log_warning
from script.oracle_pipeline import OraclePipeline




def main():
    setup_logger()
    log_info('Starting script..')
    Pipeline = OraclePipeline()
    Pipeline.run_pipeline('ARTSTL')

main()