import re
import time

from cli.help_dict import COMMANDS
from cli.regex_patterns import *
from hbase.hbase import Hbase


class CommandLineInterface:
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

                while True:
                    try:
                        user_input = input(f"hbase(main):{n_line:03d}:0> ")
                        if user_input == "exit":
                            break
                        elif user_input == "help":
                            for command, (usage, description) in COMMANDS.items():
                                print(f"{command}:\n\tUsage: {usage}\n\tDescription: {description}\n")
                        elif re.match(CREATE_PATTERN, user_input):  # Create
                            start = time.time()
                            table_name, column_families = re.match(CREATE_PATTERN, user_input).groups()

                            column_families = column_families.split(", ")
                            column_families = [cf[1:-1] for cf in column_families]  # Remove quotes

                            # If it fails, an exception is thrown and caught
                            hbase.create_table(table_name, column_families)

                            # Success
                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                            print()
                            print(f"=> Hbase::Table - {table_name}")
                        elif re.match(LIST_PATTERN, user_input):  # List
                            start = time.time()
                            regex = re.match(LIST_PATTERN, user_input).group(1)  # Gets the optional regex

                            tables = hbase.list_tables(regex)

                            print("TABLE")
                            for table in tables:
                                print(table)

                            end = time.time()
                            print(f"{len(tables)} row(s) in {end - start:.4f} seconds")
                        elif re.match(DISABLE_PATTERN, user_input):  # Disable
                            start = time.time()
                            table_name = re.match(DISABLE_PATTERN, user_input).group(1)

                            hbase.disable_table(table_name)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(ENABLE_PATTERN, user_input):  # Enable
                            start = time.time()
                            table_name = re.match(ENABLE_PATTERN, user_input).group(1)

                            hbase.enable_table(table_name)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(IS_ENABLED_PATTERN, user_input):  # Is Enabled
                            start = time.time()
                            table_name = re.match(IS_ENABLED_PATTERN, user_input).group(1)

                            print(hbase.is_table_enabled(table_name))

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(IS_DISABLED_PATTERN, user_input):  # Is Enabled
                            start = time.time()
                            table_name = re.match(IS_DISABLED_PATTERN, user_input).group(1)

                            print(hbase.is_table_disabled(table_name))

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(ALTER_PATTERN, user_input):  # Alter
                            start = time.time()
                            match = re.match(ALTER_PATTERN, user_input)

                            table_name = match.group(1)
                            dicts_string = match.group(2)
                            dict_strings = re.findall(r"\{.*?\}", dicts_string)
                            # Initialize an empty list to hold the dictionaries
                            dictionaries = []
                            # For each dictionary string
                            for dict_string in dict_strings:
                                # Remove the curly braces and split by '=>'
                                pairs = dict_string[1:-1].split(', ')
                                dictionary = {}
                                for pair in pairs:
                                    # Split the pair by '=>'
                                    key, value = pair.split(' => ')
                                    value = value.strip("'")
                                    # Add the key-value pair to the dictionary
                                    dictionary[key] = value
                                dictionaries.append(dictionary)

                            for dictionary in dictionaries:
                                hbase.alter_table(table_name, dictionary)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(DROP_PATTERN, user_input):  # Drop
                            start = time.time()
                            table_name = re.match(DROP_PATTERN, user_input).group(1)

                            hbase.drop_table(table_name)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(DROP_ALL_PATTERN, user_input):  # Drop All
                            start = time.time()
                            regex = re.match(DROP_ALL_PATTERN, user_input).group(1)  # Gets the regex

                            n_rows = hbase.drop_all_tables(regex)

                            end = time.time()
                            print(f"{n_rows} table(s) dropped in {end - start:.4f} seconds")
                        elif re.match(DESCRIBE_PATTERN, user_input):  # Describe
                            start = time.time()
                            table_name = re.match(DESCRIBE_PATTERN, user_input).group(1)

                            table_description, n_rows = hbase.describe_table(table_name)

                            print(table_description)
                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(PUT_PATTERN, user_input):  # Put
                            start = time.time()
                            match = re.match(PUT_PATTERN, user_input)
                            table_name = match.group(1)
                            rest = match.group(2)
                            entries = re.findall(PUT_BODY_PATTERN, rest)

                            for row_key, cell, value in entries:
                                cf, cq = cell.split(':')
                                hbase.put(table_name, row_key, cf, cq, value)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(GET_PATTERN, user_input):  # Get
                            start = time.time()
                            match = re.match(GET_PATTERN, user_input)

                            table_name = match.group(1)
                            row_key = match.group(2)
                            cf, cq = match.group(3).split(':') if match.group(3) else (None, None)

                            result, n_rows = hbase.get_row(table_name, row_key, cf, cq)
                            print(result)

                            end = time.time()
                            print(f"{n_rows} row(s) in {end - start:.4f} seconds")
                        elif re.match(SCAN_PATTERN, user_input):  # Scan
                            start = time.time()
                            match = re.match(SCAN_PATTERN, user_input)

                            table_name = match.group(1)

                            print(hbase.scan(table_name))

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(DELETE_PATTERN, user_input):  # Delete
                            start = time.time()
                            match = re.match(DELETE_PATTERN, user_input)

                            table_name = match.group(1)
                            row_key = match.group(2)
                            cell = match.group(3)
                            cf, cq = cell.split(':')

                            hbase.delete(table_name, row_key, cf, cq)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(DELETE_ALL_PATTERN, user_input):  # Delete All
                            start = time.time()
                            match = re.match(DELETE_ALL_PATTERN, user_input)

                            table_name = match.group(1)
                            row_key = match.group(2)

                            n_rows = hbase.delete_all(table_name, row_key)

                            end = time.time()
                            print(f"{n_rows} row(s) in {end - start:.4f} seconds")
                        elif re.match(COUNT_PATTERN, user_input):  # Count
                            start = time.time()
                            match = re.match(COUNT_PATTERN, user_input)

                            table_name = match.group(1)

                            print(hbase.count(table_name))

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        elif re.match(TRUNCATE_PATTERN, user_input):  # Truncate
                            start = time.time()
                            match = re.match(TRUNCATE_PATTERN, user_input)

                            table_name = match.group(1)

                            hbase.truncate_table(table_name)

                            end = time.time()
                            print(f"0 row(s) in {end - start:.4f} seconds")
                        else:
                            print(f"Unknown command: '{user_input}'. Try 'help'.")

                        n_line += 1
                    except KeyboardInterrupt:
                        break
                    except Exception as e:
                        print(f"Error: {e}")
        except KeyboardInterrupt:  # Catch Ctrl+C by exiting the program
            print("Bye!")
