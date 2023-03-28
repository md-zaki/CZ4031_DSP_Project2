
from collections import deque
import psycopg2

def get_exp(node):
    # Function to return explanation of a certain node type
    match node['Node Type']:
        
        case "Seq Scan":
            exp = "A Sequential Scan was executed on relation " 
            if 'Relation Name' in node:
                exp = exp + node['Relation Name']
            if 'Alias' in node:
                exp = exp + " with an Alias of " + node['Alias']
            if 'Filter' in node:
                exp = exp + " and filtered by" + node['Filter']
            return exp
        
        case "Index Scan":
            exp = "Index Scan was performed on Index of" + node['Index Name']
            if "Index Cond" in node:
                exp = exp + " with the condition " + node['Index Cond']
            if "Filter" in node:
                exp = exp + " and filtered by " + node['Filter']
            return exp
        
        case "Bitmap Heap Scan":
            exp = "With result from Bitmap Index Scan, Bitmap Heap Scan was executed on relation " + node['Relation Name'] + " matching the condition " + node['Recheck Cond'] 
            return exp
        
        case "Bitmap Index Scan":
            exp = "Bitmap Index Scan was executed on " + node['Index Name'] + " with condition of " + node['Index Cond']
            return exp
        
        case "Hash Join":
            exp = "The result from previously executed operations was joined using Hash Join"
            if "Hash Cond" in node:
                exp = exp + " with condition " + node["Hash Cond"]
            return exp
        
        case "Merge Join":
            exp = "Merge Join is executed on results of sub operations"
            if "Merge Cond" in node:
                exp = exp + " with condition " + node["Merge Cond"]
            return exp
        
        case "Nested Loop":
            exp = "Nested loop was executed to join results of sub operations"
            return exp
        
        case "Sort":
            exp = "The result is sorted using the key " + node['Sort Key']
            if "DESC" in node['Sort Key']:
                exp = exp + " in descending order"
            if "INC" in node['Sort Key']:
                exp = exp + " in ascending order"
            return exp

        case "Group":
            exp = "The result of a previous operation in Grouped by the following key/keys:"
            for i, key in enumerate(node["Group Key"]):
                exp = exp + key + " "
                if i == len(node["Group Key"]) - 1:
                    exp = exp + "."
                else:
                    exp = exp + ", "
            return exp     

        case "Limit":
            exp = "A scan was executed with a Limit of " + node['Plan Rows'] + ' entries'
            return exp

        case "Aggregate":
            exp = ''
            return exp

        case "Materialize":
            exp = "Materialized operation was executed by taking results of previous operations and storing in physical memory for faster/easier access"
            return exp
        case "Subquery Scan":
            exp = "Subquery Scan was executed on results from sub operations"
            return exp
        case "Unique":
            exp = "A scan was executed on previous operations result to remove non-unique values and keep unique values"
            return exp
        
        case "Append":
            exp = "An Append operation was executed. This combines the results of multiple operations into a single result set"
            return exp
        case "CTE Scan":
            exp = "A CTE Scan was executed on the relation " + node['CTE Name']
            if "Index Cond" in node:
                exp = exp + " with condition " + node['Index Cond']
            if "Filter" in node:
                exp = exp + " and filtered by " + node["Filter"]
            return exp
            
        case "Function Scan":
            exp = "The Function " + node['Function Name'] + " was executed and returns result as a set of rows"
        case "SetOp":
            exp = "SetOp operation with the " + node["Command"] + " command was executed between 2 scanned relations"
            return exp

        case "WindowAgg":
            exp = "Executed a window function on a set of rows"
            return exp

        case "Values Scan":
            exp = "Value Scan was executed using values declared in the query"
            return exp
            
        case "Index Only Scan":
            exp = "Index scan was execured using the index " + node['Index Name']
            if "Index Cond" in node:
                exp = exp + " with condition " + node['Index Cond']
            if "Filter" in node:
                exp = exp + " and filtered by " + node['Filter'] + '. '
            exp = exp + "Results are returned for matches."
            return exp
        case "Modify Table":
            exp = "Contents of the table was modified using Insert or Delete operations"
            return exp
        
        case "Hash":
            exp = "Hash function was executed to make memory hash using table rows"
            return exp
        case "Memoize":
            exp = "Previous result of sub operations was cached using cache key of " + node['Cache Key'] + " using the Memoized Operation"
            return exp
            
        case "Gather Merge":
            exp = "Gather Merge operation was executed on the results from parallel sub operations."
            return exp
            
        case "Gather":
            exp = "Gather operation was executed on the results from parallel sub operations."
            return exp
        case _:
            return node['Node Type']
            
def main():

    # connect to postgres
    conn = psycopg2.connect(database="postgres",
                            host="localhost",
                            user="postgres",
                            password="dspproject123",
                            port="5432")
    cursor = conn.cursor()

    extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) "
    # Specify queries to be explained here
    print("Key in Query: ")
    query = input()
    cursor.execute(extract_qp + query)

    # get query plan in JSON format
    qep = cursor.fetchall()[0][0][0].get("Plan")
    
    # make lists of nodes and its sub plans
    node_list = []
    q = deque([qep])
    while q:
        for i in range(len(q)):
            node = q.popleft()
            node_list.append(node)
            if "Plans" in node:
                for child in node['Plans']:
                    q.append(child)
    # Reverse the list
    node_list.reverse()

    # Print Executed Query
    print()
    print("Query Executed: " + query)
    print()

    # Print Query Execution Plan Tree from Postgres
    extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) "
    cursor.execute(extract_qp + query)
    qep_list1 = cursor.fetchall()
    print("Query Execution Plan Tree from Postgres:")
    for i in qep_list1:
        print(i[0])
    print()
    
    # Print Explanation
    count = 1
    print("Description: ")
    for i in node_list:
        print(str(count) + ". " + get_exp(i))
        print()
        count = count + 1

if __name__ == "__main__":
    main()

