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
            if action == "initialize_inventory":
                self.handle_initalize_inventory(data)
            elif action == "insert_money":
                self.handle_insert_money()
            elif action == "show_items":
                #also keep track of inventory
                self.handle_initalize_inventory(data)
            elif action == "select_item":
                pass
            elif action == "refund":
                pass
            else:
                print(f"⚠️ Unknown message type: {msg_type}, full data: {data}")
        self.producer.flush()
        self.consumer.flush()
        self.consumer.close()
        self.producer.close()


    
    def handle_insert_money(self, data):
        amount = data.get("amount")
        self.balance += amount
        print(f"Consumer -> New Balance of {self.balance}")


    def handle_bank(bank_msg):
        # Example: {"type": "bank", "balance": 50}
        print(f"🏦 Updating bank balance -> New balance: {bank_msg['balance']}")

 

if __name__ == "__main__":
    vending_machine_consumer = VendingMachineConsumer()
    vending_machine_consumer.run_consumer()