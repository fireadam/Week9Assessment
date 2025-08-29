from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

class VendingMachineConsumer(): 
    def __init__(self):
        self.consumer = KafkaConsumer(
            "vendingmachine",
            bootstrap_servers="localhost:29092",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="vending-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        self.balance = 0.0
        self.inventory = {
            'A1': {'name': 'Nerds Gummy Clusters', 'price': 3.95, 'quantity': 10},
            'B2': {'name': 'Dove Dark Chocolate', 'price': 5.75, 'quantity': 5},
            'C3': {'name': 'Quest Protein Chips (Ranch)', 'price': 4.50, 'quantity': 0}
        }

    
    def run_consumer(self):
 

       
        print('Consumer -> Running...')
        for msg in self.consumer:
            print(f'Consumer -> handling {msg}')
            data = msg.value  # producer should send JSON 
            if not data:
                continue

            # Producer should have a key to specify type, either 'order' or 'bank'
            msg_type = data.get("type")

            action = data.get("action")

            if action == "insert_money":
                self.handle_insert_money(data)
            elif action == "show_items":
                #also keep track of inventory
                self.handle_show_items()
            elif action == "select_item":
                self.handle_select_item(data)
            elif action == "refund":
                if self.balance > 0:
                    print(f"Money refunded: {self.balance}")
                    self.balance = 0
                    
            else:
                print(f"⚠️ Unknown message type: {msg_type}, full data: {data}")
       
        self.consumer.close()


    def handle_show_items(self):
        print("Showing Items: ")
        for name, item in self.inventory.items():
            print(f"{name}: {item['quantity']} available at ${item['price']}")

    def handle_insert_money(self, data):
        amount = data.get("amount")
        self.balance += amount
        print(f"Consumer -> New Balance of {self.balance}")



    def handle_select_item(self, data):
        item_code = data.get('item')
        if item_code not in self.inventory:
            print(f"Consumer -> {item_code} does not exist.")
        elif self.inventory[item_code]['quantity'] <= 0: 
            print(f"Consumer -> Ran out of {self.inventory[item_code]}, sorry!")
        elif self.inventory[item_code]['price'] > self.balance:
            print(f"Consumer -> Not enough $$$ inserted. Please insert at least {self.inventory[item_code]['price'] - self.balance}") 
        else:
            self.inventory[item_code]['quantity'] -= 1
            self.balance -= self.inventory[item_code]['price']
            print(f"Consumer -> Here is one {self.inventory[item_code]}, enjoy!")
            print(f"Consumer -> Here's the change:\n{self.balance}")
            self.balance = 0 


if __name__ == "__main__":
    vending_machine_consumer = VendingMachineConsumer()
    vending_machine_consumer.run_consumer()