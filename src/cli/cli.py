import re

from src.cli.regex_patterns import *
from src.hbase.hbase import Hbase


def show_help():
    commands = {"create": ("create 'table', 'column family'", "Creates a new table with the specified column family.",),
                "list": ("list", "Lists all tables in HBase."),
                "disable": ("disable 'table'", "Disables the specified table."),
                "is_enabled": ("is_enabled 'table'", "Checks if the specified table is enabled.",),
                "alter": ("alter 'table', ...", "Alters the configuration of an existing table.",),
                "drop": ("drop 'table'", "Deletes a table in HBase."),
                "drop_all": ("drop_all 'regex'", "Deletes all tables matching the regex."),
                "describe": ("describe 'table'", "Provides the description of the table."), "put": (
            "put 'table', 'row', 'column', 'value'", "Puts a cell value at the specified [row,column] in the table.",),
                "get": ("get 'table', 'row'", "Gets the contents of a row or cell."),
                "scan": ("scan 'table'", "Scans and returns the table's data."),
                "delete": ("delete 'table', 'row', 'column'", "Deletes a cell value in a table.",),
                "delete_all": ("delete_all 'table', 'row'", "Deletes all cells in a given row.",),
                "count": ("count 'table'", "Counts and returns the number of rows in a table."),
                "truncate": ("truncate 'table'", "Disables, drops and recreates the specified table.",), }

    for command, (usage, description) in commands.items():
        print(f"{command}:\n\tUsage: {usage}\n\tDescription: {description}\n")


class CommandLineInterface:
    def __init__(self):
        pass

    def run(self):
        try:
            hbase = Hbase(data_dir="hbase/data")

            while True:
                temp_input = input("$ ")

                # Wait until the user accesses the hbase shell
                if temp_input != "hbase shell":
                    print("Please access the hbase shell first by typing 'hbase shell'")
                    continue

                print("HBase Shell; enter 'help<RETURN>' for list of supported commands.")
                print('Type "exit<RETURN>" to leave the HBase Shell')

                # HBase Shell Command Loop
                n_line = 0  # Line number (amount of commands)
                try:
                    while True:
                        user_input = input(f"hbase(main):{n_line:03d}:0> ")
                        if user_input == "exit":
                            break
                        elif user_input == "help":
                            show_help()
                        elif re.match(CREATE_PATTERN, user_input):  # Create
                            table_name, column_families = re.match(CREATE_PATTERN, user_input).groups()
                            column_families = column_families.split(", ")
                            column_families = [cf[1:-1] for cf in column_families]  # Remove quotes

                            # If it fails, an exception is thrown and caught
                            hbase.create_table(table_name, column_families)

                            # Success
                            print(f"Table '{table_name}' created successfully.")

                            continue
                        elif re.match(LIST_PATTERN, user_input):  # List
                            regex = re.match(LIST_PATTERN, user_input).group(1)  # Gets the optional regex

                            tables = hbase.list_tables(regex)

                            print("TABLE")
                            for table in tables:
                                print(table)
                            print(f"{len(tables)} row(s)")
                            continue
                        elif re.match(DISABLE_PATTERN, user_input):  # Disable
                            table_name = re.match(DISABLE_PATTERN, user_input).group(1)

                            hbase.disable_table(table_name)

                            print(f"0 row(s)")
                            continue
                        elif re.match(IS_ENABLED_PATTERN, user_input):  # Is Enabled
                            # match = re.match(IS_ENABLED_PATTERN, user_input)
                            # table_name = match.group(1)
                            # result = hbase.is_table_enabled(table_name)
                            # print(result)
                            continue
                        elif re.match(ALTER_PATTERN, user_input):  # Alter
                            # TODO: Implement alter command
                            continue
                        elif re.match(DROP_PATTERN, user_input):  # Drop
                            # TODO: Implement drop command
                            continue
                        elif re.match(DROP_ALL_PATTERN, user_input):  # Drop All
                            # TODO: Implement drop_all command
                            continue
                        elif re.match(DESCRIBE_PATTERN, user_input):  # Describe
                            # match = re.match(DESCRIBE_PATTERN, user_input)
                            # table_name = match.group(1)
                            # result = hbase.describe_table(table_name)
                            # print(result)
                            continue
                        elif re.match(PUT_PATTERN, user_input):  # Put
                            # TODO: Implement put command
                            continue
                        elif re.match(GET_PATTERN, user_input):  # Get
                            # TODO: Implement get command
                            continue
                        elif re.match(SCAN_PATTERN, user_input):  # Scan
                            # TODO: Implement scan command
                            continue
                        elif re.match(DELETE_PATTERN, user_input):  # Delete
                            # TODO: Implement delete command
                            continue
                        elif re.match(DELETE_ALL_PATTERN, user_input):  # Delete All
                            # TODO: Implement delete_all command
                            continue
                        elif re.match(COUNT_PATTERN, user_input):  # Count
                            # TODO: Implement count command
                            continue
                        elif re.match(TRUNCATE_PATTERN, user_input):  # Truncate
                            # TODO: Implement truncate command
                            continue
                        else:
                            print(f"Unknown command: '{user_input}'. Try 'help'.")

                        n_line += 1
                except KeyboardInterrupt:  # Catch Ctrl+C by leaving the HBase Shell
                    break
                except Exception as e:
                    print(f"Error: {e}")
        except KeyboardInterrupt:  # Catch Ctrl+C by exiting the program
            print("Bye!")
