import json
import os
import uuid

from src.hbase.table_dataclasses import *
from src.hbase.table_decorators import update_timestamp


class Table:
    def __init__(self, table_name: str = None, column_families: list[str] = None):
        self.metadata = MetaData(  # Create a Metadata object
            name=table_name,
            column_families=[ColumnFamily(name=cf) for cf in column_families],
            id=str(uuid.uuid4()), is_disabled=False,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            n_rows=0,
        )

        self.data: List[RowEntry] | None = None  # Create a list of RowEntry objects

    def load(self, file_path: str) -> None:
        with open(file_path, "r") as f:
            data = json.load(f)

        self.metadata = MetaData(
            name=data["metadata"]["name"],
            column_families=data["metadata"]["column_families"],
            id=data["metadata"]["id"],
            is_disabled=data["metadata"]["is_disabled"],
            created_at=datetime.fromisoformat(data["metadata"]["created_at"]),
            updated_at=datetime.fromisoformat(data["metadata"]["updated_at"]),
            n_rows=data["metadata"]["n_rows"],
        )

        self.data = []

    def to_json(self) -> str:
        # Convert the datetime objects to strings
        metadata_dict = self.metadata.__dict__.copy()
        metadata_dict["created_at"] = self.metadata.created_at.isoformat()
        metadata_dict["updated_at"] = self.metadata.updated_at.isoformat()
        json_str = {"metadata": metadata_dict, "data": [], }
        if self.data:
            # Convert the datetime objects in each row to strings
            for row in self.data:
                row_dict = row.__dict__.copy()
                row_dict["timestamp"] = row.timestamp.isoformat()
                json_str["data"].append(row_dict)

        return json.dumps(json_str, indent=4)

    def save(self, save_dir: str) -> None:
        # Check if directory exists, if not, create it
        os.makedirs(save_dir, exist_ok=True)

        path = os.path.join(save_dir, f"{self.metadata.name}.json")
        with open(path, "w") as f:
            f.write(self.to_json())

    @update_timestamp
    def enable(self) -> None:
        self.metadata.is_disabled = False

    @update_timestamp
    def disable(self) -> None:
        self.metadata.is_disabled = True
