import apache_beam as beam
from apache_beam import pvalue
from domain.exception import AirbnbValidationException
from domain.airbnb_data_class import AirbnbRecord
import logging

logger = logging.getLogger(__name__)

class ValidationAirbnbRecord(beam.DoFn):
    def process(self, record: AirbnbRecord):
        try: 
            errors = record.validate()
            
            if errors: 

                raise AirbnbValidationException(errors)
            
            logger.debug(f"Valid record: {record}")

            yield record
        
        except AirbnbValidationException as e:

            #logger.error(f"Validation failed for record: {errors}")
            yield beam.pvalue.TaggedOutput("invalid", {"record": record, "errors": e.errors})

            
