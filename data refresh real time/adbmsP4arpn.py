from azure.cosmos import CosmosClient, PartitionKey
import pyodbc 
import json

# Connecting to CosmosClient using azure.cosmos library
url = 'https://adbmsp4.documents.azure.com:443/'
key = '1Pv1xkTibTL2mYUAq5gExwPwcXTkmASmPMsqXlGgsMlrRxtSmCY0WOmYmTjUWh22H26DD4e3f1CFACDb8P3izA=='
client = CosmosClient(url, credential=key)
database_name = 'adbmsp4'
database = client.get_database_client(database_name)
container_name = 'pramita'
container = database.get_container_client(container_name)

print("Conencted to CosmosDB container")

# SQL Query to create JSON of the relational data
query = """SELECT RestaurantNode.RestaurantId, RestaurantNode.RestaurantName, OrderDetails
FROM RestaurantNode
CROSS APPLY (
    SELECT OrderEdge.OrderId, ItemNode.Item, ItemNode.Price, CustomerNode.firstName, CustomerNode.lastName, CustomerNode.email, CustomerNode.address, CustomerNode.contactNumber, StatusNode.OrderStatus
    FROM OrderEdge
    JOIN ItemNode ON OrderEdge.ItemId = ItemNode.ItemId
    JOIN CustomerNode ON OrderEdge.CustomerId = CustomerNode.customerid
    JOIN StatusNode ON OrderEdge.OrderStatusId = StatusNode.OrderStatusId
    WHERE OrderEdge.RestaurantId = RestaurantNode.RestaurantId
    FOR JSON PATH
) o (OrderDetails)
FOR JSON PATH; """

# Connecting to SQL Server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=p4group1.database.windows.net;'
                      'Database=p4database;'
                      'uid=Nishi;'
                      'pwd=India@1129')

cursor = conn.cursor()

cursor.execute(query)
print("Query executed")

cursor = conn.cursor()
print("Inserted into cursor")

cursor.execute(query)

print("Query executed")
#Generating string of the json 
json_str = ''
for i in cursor:
    json_str = json_str + i[0]


#Transformating json string
json_data = json.loads(json_str)

print("Converted SQL Output to json")
print("Ingesting data into CosmosDB started")
#Inserting documents into CosmosDB
for document in json_data:
    container.upsert_item(document)

print("Ingesting data into CosmosDB completed")



