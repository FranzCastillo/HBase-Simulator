# DDL: Data Definition Language
CREATE_PATTERN = r"^create\s+'(\w+)'\s*,\s*((?:'\w+'\s*,\s*)*'\w+')"

LIST_PATTERN = r"^list(?:\s+'([^']*)')?\s*$"

DISABLE_PATTERN = r"^disable\s+'(\w+)'$"

ENABLE_PATTERN = r"^enable\s+'(\w+)'$"

IS_ENABLED_PATTERN = r"^is_enabled\s+'(\w+)'$"

IS_DISABLED_PATTERN = r"^is_disabled\s+'(\w+)'$"

ALTER_PATTERN = r"^alter\s+'(\w+)'\s*,\s*(\{[^{}]+\}(?:\s*,\s*\{[^{}]+\})*)$"

DROP_PATTERN = r"^drop\s+'(\w+)'$"

DROP_ALL_PATTERN = r"^drop_all\s+'([^']*)'\s*$"

DESCRIBE_PATTERN = r"^describe\s+'(\w+)'$"

# DML: Data Manipulation Language
PUT_PATTERN = r"^put\s+'(\w+)'\s*,\s*(.*)$"

PUT_BODY_PATTERN = r"'(\w+)'\s*,\s*'(\w+:\w+)'\s*,\s*'([^']*)'"

GET_PATTERN = r"^get\s+'(\w+)'\s*,\s*'(\w+)'\s*(?:,\s*\{COLUMN\s*=>\s*'(\w+:\w+)'\s*\})?$"

SCAN_PATTERN = r"^scan\s+'(\w+)'"

DELETE_PATTERN = r"^delete\s+'(\w+)'\s*,\s*'(\w+)'\s*,\s*'(\w+:\w+)'\s*"

DELETE_ALL_PATTERN = r"^delete_all\s+'(\w+)'\s*,\s*'(\w+)'\s*"

COUNT_PATTERN = r"^count\s+'(\w+)'"

TRUNCATE_PATTERN = r"^truncate\s+'(\w+)'"
