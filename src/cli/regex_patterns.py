# DDL: Data Definition Language
CREATE_PATTERN = r"^create\s+'(\w+)'\s*,\s*((?:'\w+'\s*,\s*)*'\w+')"

LIST_PATTERN = r"^list(?:\s+'([^']*)')?\s*$"

DISABLE_PATTERN = r"^disable\s+'(\w+)'$"

ENABLE_PATTERN = r"^enable\s+'(\w+)'$"

IS_ENABLED_PATTERN = r"^is_enabled\s+'(\w+)'$"

ALTER_PATTERN = r"^alter\s+'(\w+)',\s*'(\w+)'$"

DROP_PATTERN = r"^drop\s+'(\w+)'$"

DROP_ALL_PATTERN = r"^drop_all$"

DESCRIBE_PATTERN = r"^describe\s+'(\w+)'$"

# DML: Data Manipulation Language
PUT_PATTERN = r"^put"

GET_PATTERN = r"^get"

SCAN_PATTERN = r"^scan"

DELETE_PATTERN = r"^delete"

DELETE_ALL_PATTERN = r"^delete_all"

COUNT_PATTERN = r"^count"

TRUNCATE_PATTERN = r"^truncate"
