import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions,StandardOptions

from demos.join.join import Join
import random
from apache_beam.io.avroio import WriteToAvro

try:
    from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
    from avro.schema import parse as Parse  # avro library for python2

from fastavro import parse_schema
import json
import logging

dataflow_options = ['--project=query-11','--job_name=amaz','--temp_location=gs://dataflow_s/tmp','--region=us-central1']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# gcloud_options.job_name = "test-job"


options.view_as(StandardOptions).runner = "dataflow"


input_filename = "gs://dataflow_s/RPM/account_id_schema_new.avro"
output_filename = "gs://dataflow_s/RPM/account_id_schema_output.avro"


def printfn(elem):
    print(elem)


def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    # parser.add_argument('--project', type=str, required=False, help='project')
    parser.add_argument(
        '--records',
        dest='records',
        type=int,
        # default='gs://dataflow-samples/shakespeare/kinglear.txt',
        default='10',  # gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt
        help='Number of records to be generate')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='./',
        help='Output file to write results to.')
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Store the CLI arguments to variables
    # project_id = known_args.project

    # Setup the dataflow pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = True
    # google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # google_cloud_options.project = project_id

    save_main_session = True
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session



    SCHEMA = {"namespace": "example.avro",
              "type": "record",
              "name": "User",
              "fields": [
                  {"name": "ACNO", "type": ["null", {"logicalType": "char", "type": "string", "maxLength": 20}]},
                  {"name": "FIELD_1", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]},
                  {"name": "FIELD_2", "type": ["null", {"logicalType": "char", "type": "float", "maxLength": 20}]}

              ]
              }

    rec_cnt = known_args.records
    with beam.Pipeline(options=pipeline_options) as p:
        left_pcol_name = 'p1'
        file = p | 'read_source' >> beam.io.ReadFromAvro("./data/account_id_schema_new.avro")
        p1 = file | beam.Map(lambda x: {'ACNO':x['ACNO'],'FIELD_1':x["FIELD_1"]})
        p2 = file | beam.Map(lambda x: {'ACNO': x['ACNO'], 'FIELD_2': x["FIELD_2"]})

        # P1_1 = p1 | "write" >> beam.io.WriteToText('./data.csv')
        # P2_2 = p2 | "write2" >> beam.io.WriteToText('./data2.csv')

        right_pcol_name = 'p2'

        join_keys = {
            left_pcol_name: [
                'ACNO'
                # 't1_col_B'
            ],
            right_pcol_name: [
                'ACNO'
                # 't2_col_B'
            ]}

        pipelines_dictionary = {left_pcol_name: p1, right_pcol_name: p2}
        test_pipeline = pipelines_dictionary | 'left join' >> Join(left_pcol_name=left_pcol_name, left_pcol=p1,
                                                                   right_pcol_name=right_pcol_name, right_pcol=p2,
                                                                   join_type='left', join_keys=join_keys)
        print(type(test_pipeline))
        # test_pipeline | "print" >> beam.io.WriteToText('./test.csv')

        compressIdc = True
        use_fastavro = True
        #

        test_pipeline | 'write_fastavro' >> WriteToAvro(
            known_args.output,
            parse_schema(SCHEMA),
            use_fastavro=use_fastavro,
            file_name_suffix='.avro',
            codec=('deflate' if compressIdc else 'null'),
        )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()