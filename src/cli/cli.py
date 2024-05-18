def show_help():
    print("Que te ayude Dios")


class CommandLineInterface:
    def __init__(self):
        pass

    def run(self):
        try:
            while True:
                temp_input = input("$ ")

                # Wait until the user accesses the hbase shell
                if temp_input != "hbase shell":
                    print("Please access the hbase shell first by typing 'hbase shell'")
                    continue

                print("HBase Shell; enter 'help<RETURN>' for list of supported commands.")
                print("Type \"exit<RETURN>\" to leave the HBase Shell")

                # HBase Shell Command Loop
                n_line = 0  # Line number (amount of commands)
                try:
                    while True:
                        user_input = input(f"hbase(main):{n_line:03d}:0> ")

                        if user_input == "exit":
                            break
                        elif user_input == "help":
                            show_help()
                        else:
                            print(f"Unknown command: '{user_input}'. Try 'help'.")

                        n_line += 1
                except KeyboardInterrupt:  # Catch Ctrl+C by leaving the HBase Shell
                    break
        except KeyboardInterrupt:  # Catch Ctrl+C by exiting the program
            print("Bye!")
