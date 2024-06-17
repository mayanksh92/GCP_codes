from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import storage
import pandas as pd


credentials = service_account.Credentials.from_service_account_file('C:\\Users\\Akanksha\\Downloads\\erp_pipeline_key.json')

project_id = 'learning-gcp-424417'
client = bigquery.Client(credentials= credentials,project = project_id)


query_job = client.query("""
     CREATE TABLE mayank_erp.master_table AS select 
 o.orderid
, o.customerid
, o.employeeid
, o.orderdate
, o.shipperid
, od.orderdetailid
, od.productid
, od.quantity
, p.productname
, p.supplierid
, p.categoryid
, p.unit
, p.price
, c.customername
, c.contactname
, c.address
, c.city
, c.country
, s.suppliername
, s.contactname as seller_contact
, s.address as seller_address
, s.city as seller_city
, s.country as seller_country
, ca.categoryname
, ca.description
from `mayank_erp.orders` o 
left outer join `mayank_erp.order_details` od on o.OrderID = od.OrderID
left outer join `mayank_erp.product` p on od.ProductID = p.ProductID
left outer join `mayank_erp.customer` c on o.CustomerID = c.CustomerID
left outer join `mayank_erp.supplier` s on p.SupplierID = s.SupplierID
left outer join `mayank_erp.categories` ca on p.CategoryID = ca.CategoryID
    """)

results = query_job.result() # Wait for the job to complete.
print(results)

