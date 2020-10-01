from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.internal.clients import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

table_spec = bigquery.TableReference(
    projectId='query-11',
    datasetId='rpm',
    tableId='nana')

dataflow_options = ['--project=query-11','--job_name=amaz','--temp_location=gs://dataflow_s/tmp','--region=us-central1']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

options.view_as(StandardOptions).runner = "dataflow"

table_schema = {
    'fields': [
        {'name': 'ACNO', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_4', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_5', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_6', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_7', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_8', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_9', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FIELD_10', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}



with beam.Pipeline(options=options) as p:
    quotes = p | beam.Create([
        {'ACNO': 'Mahatma Gandhi', 'FIELD_1': '20','FIELD_2': '30','FIELD_3': '60.','FIELD_4': '50','FIELD_5': '70','FIELD_6': '90','FIELD_7': '77','FIELD_8': '99','FIELD_9': '44','FIELD_10': '20'},
        {'ACNO': 'Mahatma Gandhi', 'FIELD_1': '11','FIELD_2': '44','FIELD_3': '60.','FIELD_4': '50','FIELD_5': '70','FIELD_6': '90','FIELD_7': '77','FIELD_8': '99','FIELD_9': '44','FIELD_10': '20'},
    ])

    quotes | 'write' >> beam.io.WriteToBigQuery(
        table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
