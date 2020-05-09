import pandas
import fastavro

def avro_df(filepath):
    # Open file stream
    with open(filepath, 'rb') as fp:
        # Configure Avro reader
        reader = fastavro.reader(fp)
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        df = pandas.DataFrame.from_records(records)
        # Return created DataFrame
        return df

avro_df('/tmp/dataflow/demo/output-00000-of-00001.avro')

