import re
import sys
from io import StringIO

from ops.data.writer.feature_store import FeatureStoreWriter
from ops.data.writer.spark_writer import SparkWriter


class OutputDataFrameManager:
    def __init__(self, spark, output_df_dict, config):
        self.config = config  # Store the configuration object
        self.output_df_dict = output_df_dict  # List to store the created DataFrames
        self.spark = spark  # Store the SparkSession object

    def extract_lineage_info(self, df):
        # Get explain output
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        try:
            df.explain(True)
            explain_output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout

        sources_list = []

        # Extract JDBC source information (table and query) from the logical plan
        jdbc_pattern = re.compile(r'JDBCRelation\(\((select\s+\*\s+from\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+))\)\s+(.*?)\)',
                                  re.DOTALL)
        jdbc_matches = jdbc_pattern.findall(explain_output)

        # Process JDBC matches and extract the table and database from the SQL query
        for match in jdbc_matches:
            query = match[0]
            database_name = match[1]
            table_name = match[2]
            alias = match[3].strip()
            sources_list.append({
                'query': query,
                'database': database_name,
                'table': table_name,
                'alias': alias,
                'source Type': 'JDBC'
            })

        # Extract FileScan source information (in case of a file-based scan)
        file_scan_pattern = re.compile(r'FileScan\s+(\w+)\s+\[.*?\]\s+Location: (.*?)]', re.DOTALL)
        file_scan_matches = file_scan_pattern.findall(explain_output)
        for match in file_scan_matches:
            file_format, file_path = match
            sources_list.append({
                'File Format': file_format,
                'Path': file_path.strip(),
                'Source Type': 'FileScan'
            })

        # Remove duplicates by converting list of dictionaries to a set of tuples
        sources_list = [dict(t) for t in {tuple(d.items()) for d in sources_list}]

        sources = sources_list if sources_list else [{"Source": "Source not found"}]

        # Extract write destination information from the physical plan (if applicable)
        write_pattern = re.compile(r'Write path: (s3://[^\s]+)', re.DOTALL)
        write_matches = write_pattern.findall(explain_output)
        write_destinations = [{"Location": match} for match in write_matches] if write_matches else [
            {"Write Destination": "Write destination not found"}]

        return {
            'Sources': sources,
            'Write Destinations': write_destinations
        }

    def write_data_to_sinks(self):
        data_sinks = self.config.get("data_sinks")  # Extract data sinks
        for data_sink in data_sinks:
            input_id = data_sink.get('input_id')
            sink_name = input_id if input_id else data_sink['sink_name']
            write_df = self.output_df_dict.get(sink_name)
            sink_type = data_sink['type']
            if write_df:
                if sink_type == 'spark_sink':
                    sources = self.extract_lineage_info(write_df)
                    print(sources)
                    writer = SparkWriter(self.spark, write_df, data_sink)
                    writer.write_data()
                elif sink_type == 'pandas_sink':
                    pass
                elif sink_type == 'feature_store':
                    writer = FeatureStoreWriter(self.spark, write_df, data_sink)
                    writer.write_data()
