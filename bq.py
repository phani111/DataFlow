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
import argparse
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

table_spec = bigquery.TableReference(
    projectId='mvp-project-273913',
    datasetId='michael',
    tableId='account_id_schema_480W')

output_spec = bigquery.TableReference(
    projectId='mvp-project-273913',
    datasetId='michael',
    tableId='yesyes')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='mvp-project-273913', type=str, required=False, help='project')
    parser.add_argument('--job_name', default='rpm', type=str)
    parser.add_argument('--worker_node', default='n1-standard-16')
    parser.add_argument('--temp_location', default='gs://zz_michael/dataflow_s/tmp')
    parser.add_argument('--region', default='asia-east1')
    parser.add_argument('--staging_location', default='gs://zz_michael/dataflow_s/stage')
    parser.add_argument('--runner', default='DataflowRunner')
    parser.add_argument('--datasetid', default='michael')
    parser.add_argument('--output',required=False,default='gs://dataflow_s/RPM/account_id_schema_output.avro',help='Output file to write results to.')
    parser.add_argument('--input',default='account_id_schema_960W',help='input file to write results to.')
    parser.add_argument('--output_table', default='account_id_schema_new',
                        help='input file to write results to.')
    # Parse arguments from the command line.
    # known_args, pipeline_args = parser.parse_known_args(argv)
    args = parser.parse_args()

    dataflow_options = ['--project=%s'%(args.project), '--job_name=%s'%(args.job_name), '--temp_location=%s'%(args.temp_location),'--worker_machine_type=%s' % (args.worker_node),'--runner=%s'%(args.runner),
                        '--region=%s'%(args.region)]

    dataflow_options.append('--staging_location=%s'%(args.staging_location))
    options = PipelineOptions(dataflow_options)
    gcloud_options = options.view_as(GoogleCloudOptions)
    #
    options.view_as(StandardOptions).runner = "dataflow"

    input_filename = args.input
    output_filename = args.output

    table_spec = bigquery.TableReference(
        projectId=args.project,
        datasetId=args.datasetid,
        tableId=args.input)

    table_schema = {
        'fields': [
            {'name': 'ACNO', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FIELD_1', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_2', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_3', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_4', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_5', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_6', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_7', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_8', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_9', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'FIELD_10', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
    }



    with beam.Pipeline(options=options) as p:
        quotes = p | 'read_source' >> beam.io.Read(beam.io.BigQuerySource(table_spec))

        quotes | 'write' >> beam.io.WriteToBigQuery(
            output_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

