import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from options import AirbnbOptions
from domain.airbnb_data_class import AirbnbRecord
from domain.airbnb_mapper_func import ParseCsvToDataclass, load_schema, generate_record_id
from domain.airbnb_val_func import ValidationAirbnbRecord
from domain.logs.logging_config import logger_config
import dataclasses

logger_config()

def run() -> None:

    logger = logging.getLogger(__name__)

    logger.info("Starting Beam pipeline")

    bq_schema = {"fields": load_schema()}

    options = PipelineOptions()
    airbnb_options = options.view_as(AirbnbOptions)
    logger.info(f"Input file: {airbnb_options.input_file}")

    # Start beam pipeline
    with beam.Pipeline(options=options) as p:
        parsed_records = (p
         | "Read CSV" >> beam.io.ReadFromText(
             airbnb_options.input_file,
             skip_header_lines = 1)
         | "Parse CSV to AirbnbRecord" >> beam.ParDo(ParseCsvToDataclass(AirbnbRecord)).with_outputs("invalid", main="valid"))
        
        n_multiline_records = parsed_records.valid

        _ = (
            n_multiline_records
            | "Count valid_mulitline" >> beam.combiners.Count.Globally()
            | "Log count_valid_multi" >> beam.Map(lambda c: logger.info(f"Total multicount: {c}"))
        )
        
        # Validate Records: main=valid, side=invalid
        valid_records, invalid_records = (
         n_multiline_records | 
         "Validate AirbnbRecords" >> beam.ParDo(ValidationAirbnbRecord()).with_outputs(
             "invalid", 
             main = "valid"
         )
        )

        valid_dict_records = (valid_records | "Convert to Dict" >> beam.Map(lambda data: dataclasses.asdict(data))
                                            | "Add Record ID" >> beam.Map(
                                                lambda d: {
                                                    **d,
                                                    "_record_id": generate_record_id(d)
                                                }
                                            ))

        _ = (
            valid_records
            | "Count Valid Records" >> beam.combiners.Count.Globally()
            | "Log Valid Count" >> beam.Map(lambda c: logger.info(f"Total valid records to write to BigQuery: {c}"))
        )

        _ = (
            invalid_records
            | "Count inValid Records" >> beam.combiners.Count.Globally()
            | "Log inValid Count" >> beam.Map(lambda c: logger.info(f"Total invalid records to write to BigQuery: {c}"))
        )
        
        # Write valid records to BigQuery
        (
        valid_dict_records | 
        "Write Valid to BQ" >> beam.io.WriteToBigQuery(
            table=f"{airbnb_options.project_id}:{airbnb_options.dataset_staging}.{airbnb_options.staging_table}",
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method="FILE_LOADS"
            )  
        )
        invalid_records | "Write Invalid to GCS" >> beam.io.WriteToText(
            airbnb_options.invalid_bucket
        )




if __name__ == "__main__":
    run()