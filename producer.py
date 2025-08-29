from kafka import KafkaProducer
import json

class VendingMachineProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )



    def read_required_string(self, prompt):
        while True:
            value = input(prompt).strip()
            if value:
                return value
            print("Error: value is required.")

    def print_header(self, header):
        print()
        print(header)
        print("=" * len(header))

    def read_int_for_menu(self, prompt, min, max):
        while True:
            value = self.read_required_string(prompt)
            if value.isdigit():
                value = int(value)
                if min <= value <= max:
                    return value
            print(f"Error: Value must be between {min} and {max}.")

    def choose_menu(self):
        self.print_header("Vending Machine")
        print("0. Exit")
        print("1. Insert Money")
        print("2. Show Items")
        print("3. Select Item")
        print("4. Cancel and Refund")
        return self.read_int_for_menu("Select [0-4]: ", 0, 4)

    def insert_money(self):
       
        dollars = {
            1: ("1¢", .01), 
            2: ("5¢", .05), 
            3: ("10¢", .10),
            4: ("25¢", .25), 
            5: ("$1 coin", 1.00), 
            6: ("$1 bill", 1.00), 
            7: ("$5 bill", 5.00)
        }

        print("Select dollar amount: ")
        for key, value in dollars.items():
            print(f"{key}. {value[0]}")

        selection = self.read_int_for_menu("Select [1-7]: ", 1, 7)
        print("Produer -> Inserting money...")
        amount = dollars[selection][1]
        
        self.producer.send("vendingmachine", {"action": "insert_money", "amount": amount})
        print(f"Producer -> {amount} sent!")

    def show_items(self):
        producer = self.instantiate_producer()
        print("Showing items...")
        producer.send("vendingmachine", {"action": "show_items"})
        producer.flush()
        producer.close()

    def select_items(self):
        producer = self.instantiate_producer()
        print("Selecting item...")
        item = self.read_required_string("Enter item code: ")
        producer.send("vendingmachine", {"action": "select_item", "item": item})
        producer.flush()
        producer.close()

    def refund(self):
        producer = self.instantiate_producer()
        print("Refunding. . .")
        producer.send("vendingmachine", {"action": "refund"})
        producer.flush()
        producer.close()
        
    def menu(self):
        choice = self.choose_menu()
        while choice != 0:
            if choice == 0:
                self.producer.flush()
                self.producer.close()
                break
            if choice == 1:
                self.insert_money()
            elif choice == 2:
                self.show_items()
            elif choice == 3:
                self.select_items()
            elif choice == 4:
                self.refund()
            choice = self.choose_menu()

    def run_producer(self):
        


        #print(self.producer.bootstrap_connected())


        # Menu here
        # 1. Let users send money 
        # 2. let users select which item to buy. Decrement that specific item and send it back. 
        # Each function will make the producer and close it. 

        self.menu()
    
producer = VendingMachineProducer()
producer.run_producer() 