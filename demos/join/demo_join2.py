import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions

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
        default='10', # gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt
        help='Number of records to be generate')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='/tmp/dataflow/demo/output',
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

    save_main_session=True
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    # SCHEMA_STRING = '''
    # {"namespace": "example.avro",
    # "type": "record",
    # "name": "User",
    # "fields": [
    #     {"name": "ACNO", "type": "int"},
    #     {"name": "PRIN_BAL", "type": "int"},
    #     {"name": "FEE_ANT", "default": null, "type": ["null", "double"]},
    #     {"name": "GENDER",  "default": null, "type": ["null", {"logicalType": "char", "type": "string", "maxLength": 1}]}

    # ]
    # }
    # '''

    SCHEMA = {"namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "ACNO", "type": "int"},
        {"name": "PRIN_BAL", "type": "int"},
        {"name": "FEE_ANT", "default": 'null', "type": ["null", "double"]},
        {"name": "GENDER",  "default": 'null', "type": ["null", {"logicalType": "char", "type": "string", "maxLength": 1}]}

    ]
    }

    # {"name": "GENDER', "type": "string"}

    # {"name": "FEE_ANT", "type": "long"}

    # p = beam.Pipeline(options=pipeline_options)
    rec_cnt=known_args.records
    with beam.Pipeline(options=pipeline_options) as p:
        left_pcol_name = 'p1'
        p1 = p | 'Create source data' >> beam.Create(
                [{
                    'ACNO': i+1,
                    'PRIN_BAL': i+1,
                    'GENDER1': 'Y',
                    'GENDER': random.choice(['Y','N']),
                } for i in range(rec_cnt)]
            )

        right_pcol_name = 'p2'
        p2 = p | 'Create join data' >> beam.Create(
                [{
                    'ACNO': i+1,
                    'FEE_ANT': random.random()*100000000,
                    
                } for i in range(rec_cnt)]

            )

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

        # test_pipeline | "print" >> beam.Map(printfn)

        compressIdc=True
        use_fastavro=True

        test_pipeline | 'write_fastavro' >> WriteToAvro(
            known_args.output,
            # '/tmp/dataflow/{}/{}'.format(
            #     'demo', 'output'),
            # parse_schema(json.loads(SCHEMA_STRING)),
            parse_schema(SCHEMA),
            use_fastavro=use_fastavro,
            file_name_suffix='.avro',
            codec=('deflate' if compressIdc else 'null'),
        )
    # result = p.run()
    # result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
