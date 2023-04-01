
#conncet to mysql
import mysql.connector
import csv
# Connect to the database
cnx = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456",
  database="310323_demo_qcc"
)

# Create a cursor to execute queries
cursor = cnx.cursor()

# Execute a query
query_to_analyze = '''     select *
            from patient_appointments pa
            join package_utilization_detail pud on pud.appointment_id  = pa.id 
            join package_purchase pp on pp.id = pa.package_purchase_id 
            join location_master lm on lm.id  = pa.location_id 
            join patients p on p.id = pa.patient_id 
            join package_purchase_detail ppd on ppd.package_purchase_id  = pa.package_purchase_id
            LEFT JOIN services srv ON ppd.type = 'service' AND srv.id = ppd.item_id
            LEFT JOIN products prd ON ppd.type = 'product' AND prd.id = ppd.item_id
            where pa.patient_id  = 18887 and pa.package_purchase_id  = 439 and pa.deleted_at is null and pa.status = 'Booked'
            group by pa.id'''


tables = []
joins = []
query_to_analyze = query_to_analyze.strip()
split_query = query_to_analyze.split(" ")
for i, item in enumerate(split_query):
    if item.lower() == "from":
        tables.append(split_query[i+1])
    elif item.lower() == "join":
        join = {}
        join["table"] = split_query[i+1]
        tables.append(split_query[i+1])
        join["condition"] = split_query[i+3] + " " + split_query[i+4] + " " + split_query[i+5]
        joins.append(join)


split_query = [x for x in split_query if x != '']
for i, item in enumerate(split_query):
    if item.lower() == "join":
        if split_query[i-1].lower() == "left" or split_query[i-1].lower() == "right" or split_query[i-1].lower() == "inner" or split_query[i-1].lower() == "outer":
            split_query[i-1] = split_query[i-1] + " " + split_query[i]
            split_query.pop(i)
            #break
#remove any extra spaces
def write_results_to_csv(cursor, filename,sql_query):
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)      
        for i in range(5):
            writer.writerow([''])       
        writer.writerow(['Query: ' + sql_query])
        writer.writerow([i[0] for i in cursor.description])        
        for row in cursor.fetchall():
            writer.writerow(row)

queries = '';
index = 0
for dt in split_query:    
    if 'join' in dt:
        cursor.execute(queries)
        write_results_to_csv(cursor, 'output.csv',queries)      
        queries = queries + dt + ' '
    else:
        queries = queries + dt + ' '
    index = index + 1
cursor.execute(queries) #for the complete query
write_results_to_csv(cursor, 'output.csv',queries)

cursor.close()




    