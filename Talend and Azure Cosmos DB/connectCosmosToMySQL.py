from mysql import connector
from azure.cosmos import CosmosClient
import json
import mysql
# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Pramita@123',
    database='cosmos'
)
cursor = conn.cursor()

# SQL Query to create JSON of the relational data
query = """
    SELECT 
        JSON_OBJECT(
            'customerId', c.customerid,
            'firstName', c.first_name,
            'lastName', c.last_name,
            'orders', IFNULL(
                (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'orderId', o.orderid,
                            'restaurantName', r.restaurantname,
                            'item', i.item,
                            'price', i.price,
                            'orderStatus', s.orderstatus
                        )
                    )
                    FROM Orders o 
                    JOIN Restaurant r ON o.restaurantid = r.restaurantid 
                    JOIN Item i ON o.itemid = i.itemid 
                    JOIN Status s ON o.orderstatusid = s.orderstatusid 
                    WHERE c.customerid = o.customerid
                    GROUP BY o.orderid
                ),
                JSON_ARRAY()
            )
        ) AS customer_orders
    FROM 
        Customer c 
    ORDER BY  
        c.customerid
"""

# Execute the query and fetch the results
cursor.execute(query)
results = cursor.fetchall()
from pprint import pprint
# Transform the results into JSON
json_data = []
for row in results:
    json_data.append(json.loads(row[0]))
pprint(json_data)
# Connect to Cosmos DB
url = 'https://pramita689.documents.azure.com:443/'
key = 'uzF23pjnb4l1oPca5ZFHk7WWwkhOZE3nGQQ3qEYkxE7tFyEmNLpdaYt6u08G4jpG7umKLwihstCCACDb7q4c7A=='
client = CosmosClient(url, credential=key)
database_name = 'pramita'
database = client.get_database_client(database_name)
container_name = 'adbmsProject'
container = database.get_container_client(container_name)
print(container)
import json
with open('data.json', 'w') as f:
    json.dump(json_data, f)
# Insert the JSON data into Cosmos DB
# Insert the JSON data into Cosmos DB

for document in json_data:
    container.upsert_item(document)

print("Data imported successfully")

