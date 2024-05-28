COMMANDS = {
        "create": (
            "create '<table_name>', '<column_family_1>', '<column_family_2>', ...",
            "Creates a new table with the specified column family.",
        ),
        "list": (
            "list '<regex>'",
            "If no regex is specified, lists all tables in HBase. Otherwise, lists tables matching the regex."
        ),
        "disable": (
            "disable '<table_name>'",
            "Disables the specified table."
        ),
        "enable": (
            "enable '<table_name>'",
            "Enables the specified table."
        ),
        "is_enabled": (
            "is_enabled '<table_name>'",
            "Checks if the specified table is enabled.",
        ),
        "is_disabled": (
            "is_disabled '<table_name>'",
            "Checks if the specified table is disabled.",
        ),
        "alter": (
            "alter '<table_name>', <column_family_dict_1>, <column_family_dict_2>, ...",
            "Alters the configuration of an existing table.",
        ),
        "drop": (
            "drop '<table_name>'",
            "Deletes a table in HBase. Table must be disabled first."
        ),
        "drop_all": (
            "drop_all '<regex>'",
            "Deletes all tables matching the regex. Tables must be disabled first."
        ),
        "describe": (
            "describe '<table_name>'",
            "Provides the description of the table and its column families."
        ),
        "put": (
            "put '<table_name>', '<row_id>', '<column_family>:<column_qualifier>', '<value>'",
            "Puts a cell value at the specified [row,column] in the table.",
        ),
        "get": (
            "get '<table_name>', '<row_id>'",
            "Gets the contents of a row or cell."
        ),
        "scan": (
            "scan '<table_name>'",
            "Scans and returns the table's data."
        ),
        "delete": (
            "delete '<table_name>', '<row_id>', '<column_family>:<column_qualifier>'",
            "Deletes a cell value in a table.",
        ),
        "delete_all": (
            "delete_all '<table_name>', '<row_id>'",
            "Deletes all cells in a given row.",
        ),
        "count": (
            "count '<table_name>'",
            "Counts and returns the number of rows in a table."
        ),
        "truncate": (
            "truncate '<table_name>'",
            "Disables, drops and recreates the specified table.",
        ),
    }