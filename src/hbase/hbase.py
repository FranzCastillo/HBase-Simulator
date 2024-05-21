import json
import os
import time
import uuid

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


class Table:
    def __init__(self, table_name):
        self.table_name = table_name
        self.data_dir = "hbase/data"
        self.schema_dir = "hbase/schemas"

    def metadata(self):
        data_file = os.path.join(self.data_dir, f"{self.table_name}.avro")
        with DataFileReader(open(data_file, "rb"), DatumReader()) as reader:
            for record in reader:
                return record["metadata"]

    def put(self, row_key, data) -> str:
        # TODO: Implement put
        pass

    def scan(self) -> str:
        # TODO: Implement scan
        pass

    def close(self):
        self.writer.close()


class Hbase:
    def __init__(self):
        self.data_dir = "hbase/data"
        self.schema_dir = "hbase/schemas"

        # check if the data directory exists
        if not os.path.exists(self.data_dir):
            Exception("Data directory not found.")

        # check if the schema directory exists
        if not os.path.exists(self.schema_dir):
            Exception("Data directory not found.")

    def list_tables(self) -> str:
        tables = []
        if os.path.exists(self.data_dir):
            for table_file in os.listdir(self.data_dir):
                if table_file.endswith(".avro"):
                    table_name = table_file.split(".")[0]
                    table = Table(table_name)
                    metadata = table.metadata()
                    tables.append(
                        {
                            "name": table_name,
                            "column_families": metadata["columnFamilies"],
                            "created_at": metadata["createdAt"],
                            "num_rows": metadata["rows"],
                        }
                    )

        if tables:
            table_info = []
            for table in tables:
                info = f"Table: {table['name']}\n"
                info += f"Column Families: {', '.join(table['column_families'])}\n"
                info += f"Created At: {table['created_at']}\n"
                info += f"Number of Rows: {table['num_rows']}\n"
                table_info.append(info)
            return "\n".join(table_info)
        else:
            return "No tables found."

    def create_table(self, table_name, column_families) -> str:
        schema_file = os.path.join(self.schema_dir, "table.avsc")

        if not os.path.exists(schema_file):
            return "Schema for tables not found."

        with open(schema_file, "r") as f:
            schema = avro.schema.parse(f.read())

        # Check if the table already exists
        data_file = os.path.join(self.data_dir, f"{table_name}.avro")
        if os.path.exists(data_file):
            return f"Table {table_name} already exists."

        metadata = {
            "name": table_name,
            "columnFamilies": column_families,
            "id": str(uuid.uuid4()),
            "disabled": False,
            "createdAt": int(time.time() * 1000),
            "updatedAt": int(time.time() * 1000),
            "rows": 0,
        }

        # Create the table file
        with DataFileWriter(open(data_file, "wb"), DatumWriter(), schema) as writer:
            writer.append({"metadata": metadata, "data": []})

        return f"Table {table_name} created successfully."

    def disable_table(self, table_name) -> str:
        data_file = os.path.join(self.data_dir, f"{table_name}.avro")
        if not os.path.exists(data_file):
            return f"Table {table_name} does not exist."

        with DataFileReader(open(data_file, "rb"), DatumReader()) as reader:
            records = list(reader)
            metadata = records[0]["metadata"]
            metadata["disabled"] = True
            metadata["updatedAt"] = int(time.time() * 1000)

        with DataFileWriter(
            open(data_file, "wb"), DatumWriter(), reader.datum_reader.writers_schema
        ) as writer:
            for record in records:
                writer.append(record)

        return f"Table {table_name} disabled successfully."

    def is_table_enabled(self, table_name) -> str:
        data_file = os.path.join(self.data_dir, f"{table_name}.avro")
        if not os.path.exists(data_file):
            return f"Table {table_name} does not exist."

        with DataFileReader(open(data_file, "rb"), DatumReader()) as reader:
            metadata = next(reader)["metadata"]
            is_enabled = not metadata["disabled"]

        return f"Table {table_name} is {'enabled' if is_enabled else 'disabled'}."

    def describe_table(self, table_name) -> str:
        data_file = os.path.join(self.data_dir, f"{table_name}.avro")
        if not os.path.exists(data_file):
            return f"Table {table_name} does not exist."

        with DataFileReader(open(data_file, "rb"), DatumReader()) as reader:
            metadata = next(reader)["metadata"]

        table_info = f"Table Name: {metadata['name']}\n"
        table_info += f"Column Families: {', '.join(metadata['columnFamilies'])}\n"
        table_info += f"ID: {metadata['id']}\n"
        table_info += f"Disabled: {metadata['disabled']}\n"
        table_info += f"Created At: {metadata['createdAt']}\n"
        table_info += f"Updated At: {metadata['updatedAt']}\n"
        table_info += f"Number of Rows: {metadata['rows']}\n"

        return table_info
