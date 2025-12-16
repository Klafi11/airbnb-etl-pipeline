from apache_beam.options.pipeline_options import PipelineOptions

class AirbnbOptions(PipelineOptions): 
    """ 
    Adds custom Option functionality to Dataflow Pipeline 
    Docs: https://docs.cloud.google.com/dataflow/docs/guides/setting-pipeline-options#custom-options
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        """Overides classmethod for custom options"""
        parser.add_argument("--input_file", required = True, help = "Path to input CSV (local or GCS)")
        parser.add_argument("--project_id", default="etl-portfolio", help = "GCP Project ID" )
        parser.add_argument("--dataset_staging", default="etl_dataset_staging", help = "BigQuery dataset")
        parser.add_argument("--staging_table", default="stg_airbnb_listings_raw", help = "BigQuery table")
        parser.add_argument("--invalid_bucket", default="gs://etl-portfolio-invalid/errors", help = "Invalid Bucket")
        
        

