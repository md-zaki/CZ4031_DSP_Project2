
from collections import deque
import psycopg2
import pydot
import re

def get_exp(node):
    """
        Parameters:
            node: Representing a subplan in JSON format
        Returns:
            exp (str): Explanation in natural language of node
    """
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
            exp = "Bitmap Index Scan was executed on Index " + node['Index Name'] + " with condition of " + node['Index Cond']
            return exp
        
        case "Hash Join":
            #exp = "The result from previously executed operations was joined using Hash Join"
            exp = "The result from steps ("+ str(node['Plans'][0]['step']) +") and ("+ str(node['Plans'][1]['step']) +") was joined using Hash Join"
            if "Hash Cond" in node:
                exp = exp + " with condition " + node["Hash Cond"]
            return exp
        
        case "Merge Join":
            #exp = "Merge Join is executed on results of sub operations"
            exp = "Merge Join is executed on results of steps ("+ str(node['Plans'][0]['step']) +") and ("+ str(node['Plans'][1]['step']) +")"
            if "Merge Cond" in node:
                exp = exp + " with condition " + node["Merge Cond"]
            return exp
        
        case "Nested Loop":
            exp = "Nested loop was executed to join results of steps ("+ str(node['Plans'][0]['step']) +") and ("+ str(node['Plans'][1]['step']) +")"
            return exp
        
        case "Sort":
            #exp = "The result is sorted using the key " + str(node['Sort Key'])
            exp = "The result from step ("+ str(node['Plans'][0]['step']) +") is sorted using the key " + str(node['Sort Key'])
            if "DESC" in node['Sort Key'][0]:
                exp = exp + " in descending order"
            if "INC" in node['Sort Key'][0]:
                exp = exp + " in ascending order"
            return exp

        case "Group":
            exp = "The result from step ("+ str(node['Plans'][0]['step']) +") is Grouped by the following key/keys:"
            key = node["Group Key"]
            exp = exp + str(key) + " "
                
                    
            return exp     

        case "Limit":
            exp = "A scan was executed with a Limit of " + node['Plan Rows'] + ' entries'
            return exp

        case "Aggregate":
            if node['Strategy'] == "Hashed":
                exp = "Aggregation was executed by Hashing on all rows of relation from step ("+ str(node['Plans'][0]['step']) +") based on the following keys: "
                for i in node['Group Key']:
                    exp = exp + str(i) + ','
                exp = exp + ". The results are aggregated into buckets according to the hashed key."
                return exp
            elif node['Strategy'] == "Plain":
                exp = "Normal Aggregation was executed on the result from step ("+ str(node['Plans'][0]['step']) +")"
                return exp
            elif node['Strategy'] == 'Sorted':
                exp = "Aggregation was executed by sorting all rows of relation from step ("+ str(node['Plans'][0]['step']) +") based on keys. "
                if "Group Key" in node:#['Strategy']:
                    exp = exp + "The aggregated keys are: "
                    for i in node['Group Key']:
                        exp = exp + str(i) + ','
                return exp
            return exp

        case "Materialize":
            exp = "Materialized operation was executed by taking results step ("+ str(node['Plans'][0]['step']) +") and storing in physical memory for faster/easier access"
            return exp
        case "Subquery Scan":
            exp = "Subquery Scan was executed on results from sub operations"
            return exp
        case "Unique":
            exp = "A scan was executed on step ("+ str(node['Plans'][0]['step']) +") result to remove non-unique values and keep unique values"
            return exp
        
        case "Append":
            exp = "An Append operation was executed. This combines the results of multiple operations into a single result set"
            return exp
        case "CTE Scan":
            exp = "A CTE Scan was executed on the relation " + str(node['CTE Name'])
            if "Index Cond" in node:
                exp = exp + " with condition " + node['Index Cond']
            if "Filter" in node:
                exp = exp + " and filtered by " + node["Filter"]
            return exp
            
        case "Function Scan":
            exp = "The Function " + node['Function Name'] + " was executed and returns result as a set of rows"
        case "SetOp":
            exp = "SetOp operation with the " + str(node["Command"]) + " command was executed between 2 scanned relations"
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
            exp = "Hash function was executed on results from step (" + str(node['Plans'][0]['step']) +") to make memory hash using table rows"
            return exp
        case "Memoize":
            exp = "Previous result of sub operations was cached using cache key of " + node['Cache Key'] + " using the Memoized Operation"
            return exp
            
        case "Gather Merge":
            #exp = "Gather Merge operation was executed on the results from parallel sub operations. Order of the results is preserved."
            exp = "Gather Merge operation was executed on the results from step ("+ str(node['Plans'][0]['step']) +"). Order of the results is preserved."
            return exp
            
        case "Gather":
            exp = "Gather operation was executed on the results from step ("+ str(node['Plans'][0]['step']) +"). Order of the results is not preserved."
            return exp
        case _:
            return node['Node Type']



def compare_graph_labels(graph1_str, graph2_str):
    """
        Parameters:
            graph1_str (str): Dot string of graph 1
            graph2_str (str): Dot string of graph 2
        Returns:
            missing_in_graph1 (set): set of node types missing in graph 1
            missing_in_graph2 (set): set of node types missing in graph 2
    """
    # Parse the Graphviz graphs from the input strings
    graph1 = pydot.graph_from_dot_data(graph1_str)[0]
    graph2 = pydot.graph_from_dot_data(graph2_str)[0]

    # Get the labels of the nodes in each graph
    graph1_labels = set([node.get_label() for node in graph1.get_nodes()])
    graph2_labels = set([node.get_label() for node in graph2.get_nodes()])

    # Find the labels present in graph1 but missing in graph2
    missing_in_graph2 = graph1_labels - graph2_labels

    # Find the labels present in graph2 but missing in graph1
    missing_in_graph1 = graph2_labels - graph1_labels

    # Return the missing labels for each graph
    return missing_in_graph1, missing_in_graph2

def highlight_node(dot_string,element):
    """
        Parameters:
            dot_string (str): Dot string of graph to be highlighted
            element (str): Element to be highlighted in graph
        Returns:
            dot_string of graph with highlights
    """
    node_id_arr = []
    node_id = None
    lines = dot_string.split('\n')
    for line in lines:
        if 'label='+str(element) in line:
            node_id = line.split(' ')[0]
            node_id_arr.append(node_id)
    
    # If the node is found, add a yellow fill color to it
    if node_id is not None:
        for i in range(len(lines)):
            for node_id in node_id_arr:
                if node_id in lines[i]:
                    lines[i] = lines[i].replace(']', ' style=filled fillcolor=yellow];')
                    
    
    # Return the modified dot string
    return '\n'.join(lines)

def query_diff(q1, q2):
    """
        Parameters:
            q1 (str): string of first sql query
            q2 (str): string of second sql query
        Returns:
            comp_str (str): string of explanation of changes in the sql query
    """
    select_clause1 = ''
    from_clause1 = ''
    where_clause1 = ''
    select_clause2 = ''
    from_clause2 = ''
    where_clause2 = ''
    if(q1 == q2):
        return "There are no changes between the queries"
    comp_str = 'Due to changes in the '

    try:
        select_clause1 = re.search(r"(?i)^SELECT\s+(.+?)\s+FROM", q1).group(1)
    except:
        try:
            select_clause1 = re.search(r"(?i)^SELECT\s+(.+?)$", q1).group(1)
        except:
            pass

    try:
        from_clause1 = re.search(r"(?i)^SELECT.+?\s+FROM\s+(.+?)\s+WHERE", q1).group(1)
    except:
        try:
            from_clause1 = re.search(r"(?i)^SELECT.+?\s+FROM\s+(.+?)$", q1).group(1)
        except:
            pass

    try:
        where_clause1 = re.search(r"(?i)^SELECT.+?\s+FROM.+?\s+WHERE\s+(.+?)$", q1).group(1)
    except:
        pass

    try:
        select_clause2 = re.search(r"(?i)^SELECT\s+(.+?)\s+FROM", q2).group(1)
    except:
        try:
            select_clause2 = re.search(r"(?i)^SELECT\s+(.+?)$", q2).group(1)
        except:
            pass

    try:
        from_clause2 = re.search(r"(?i)^SELECT.+?\s+FROM\s+(.+?)\s+WHERE", q2).group(1)
    except:
        try:
            from_clause2 = re.search(r"(?i)^SELECT.+?\s+FROM\s+(.+?)$", q2).group(1)
        except:
            pass

    try:
        where_clause2 = re.search(r"(?i)^SELECT.+?\s+FROM.+?\s+WHERE\s+(.+?)$", q2).group(1)
    except:
        pass

    if str(select_clause1) != str(select_clause2):
        comp_str = comp_str + 'SELECT clause, '
    if str(from_clause1) != str(from_clause2):
        comp_str = comp_str + 'FROM clause, '
    if str(where_clause1) != str(where_clause2):
        comp_str = comp_str + 'WHERE clause, '
    comp_str = comp_str + 'of the Evolved Query '

    return comp_str


def qep_diff_exp(missing1, missing2):
    """
        Parameters:
            missing1 (set): set of operations not in QEP 1
            missing2 (set): set of operations not in QEP 2
        Returns:
            exp_str1 (str): string of explanation of changes of operations between the 2 QEPs
    """
    if(missing2):
        exp_str1 = 'The following operations from the original query plan: '
        for i in missing2:
            exp_str1 = exp_str1 + str(i) + ', '
        exp_str1 = exp_str1 + 'were replaced by '
        if(missing1):
            exp_str1 = exp_str1 + 'The following operations: '
            for i in missing1:
                exp_str1 = exp_str1 + str(i) + ', '
            exp_str1 = exp_str1 + ' in the evolved query plan'
    elif(missing1):
        exp_str1 = 'The following operations were added to the evolved query plan: '
        for i in missing1:
            exp_str1 = exp_str1 + str(i) + ', '

    
    return exp_str1

def add_steps(node_list):
    step = 1
    for node in node_list:
        node['step'] = step
        step += 1
    return node_list

def add_relation_details(node_list):    # This function adds details of relations involved in resulting tuples
    """
        Parameters:
            node_list (Dict): node_list which contains the operators of the QEP
        Returns:
            node_list (Dict): Returns the same node_list with new key 'Relations' and 'step'
    """
    scans = ["Seq Scan", "Index Scan", "Bitmap Heap Scan", "Bitmap Index Scan", "Subquery Scan", "CTE Scan", "Function Scan", "Values Scan", "Index Only Scan"]
    joins = ["Hash Join", "Nested Loop", "Merge Join"]
    #step = 1
    placeholder = 0
    for node in node_list:
        #if(node['Node Type'] in scans): #get the relation details from scans
        if node['Node Type'] == "Bitmap Index Scan":
            continue
        if node['Node Type'] == "Bitmap Heap Scan":
            node['Plans'][0]['Relations'] = [node['Relation Name']]
        if 'Relation Name' in node: #if Relation Name exists   
            node['Relations'] = [node['Relation Name']]
        elif 'Plans' in node: #should be a intermediate node (i.e. not leaf node, becus got children)
            if 'Relations' in node['Plans'][0]:
                node['Relations'] = [node['Plans'][0]['Relations']]
        else:   #should be a leaf node without Relation Name, then we give it a placeholder relation
            node['Relations'] = ["Placeholder" + str(placeholder)]
            placeholder += 1
            
        #if(node['Node Type'] not in scans and node['Node Type'] not in joins):  #intermediate nodes get relation details from child
        if 'Plans' in node and node['Node Type'] not in joins: #meaning it is a intermediate operator that is not a join operation
            node['Relations'] = node['Plans'][0]['Relations']
        if(node['Node Type'] in joins): #node is a join operator, get relation details from both chilren
            temp_relation = node['Plans'][0]['Relations']
            temp_relation1 = node['Plans'][1]['Relations']
            node['Relations'] = temp_relation+temp_relation1
        
    return node_list

def identify_same_nodes(i, j):  #This function determines whether 2 operators are the same and returns True/False
    """
        Parameters:
            i (Dict): Dictonary containing details of an operator
            j (Dict): Dictonary containing details of an operator
        Returns:
            True/False (Bool) : Returns True if the 2 operators are the same, else False
    """
 
    if(j["Node Type"]==i["Node Type"]):
                  
        match j["Node Type"]:
            case "Seq Scan":
                j_filter = None
                i_filter = None
                if 'Filter' in j: j_filter = j['Filter']
                if 'Filter' in i: i_filter = i['Filter']
                if i["Relation Name"] == j["Relation Name"] and j_filter==i_filter:
                    return True
            case "Index Scan":
                j_filter = None
                i_filter = None
                if 'Filter' in j: j_filter = j['Filter']
                if 'Filter' in i: i_filter = i['Filter']
                if i["Index Name"] == j["Index Name"] and i["Index Cond"] == j["Index Cond"] and j_filter==i_filter:
                    return True
            case "Bitmap Index Scan":
                if check_same_list(i['Relations'], j['Relations']) and i['Index Cond'] == j['Index Cond'] and i['Index Name'] == j['Index Name']:
                    return True
            case "Bitmap Heap Scan":
                if i['Relation Name'] == j['Relation Name']:
                    return True

            case "Hash Join":
                if i["Hash Cond"] == j["Hash Cond"]:
                    return True
            case "Merge Join":
                if i["Merge Cond"] == j["Merge Cond"]:
                    return True
            case "Nested Loop":
                if check_same_list(i['Relations'], j['Relations']):
                    return True

            case "Hash":
                if check_same_list(i['Relations'], j['Relations']):
                    return True
            case "Sort":
                if check_same_list(i['Relations'], j['Relations']) and i['Sort Key'] == j['Sort Key']:
                    return True
            case "Group":
                if check_same_list(i['Relations'], j['Relations']) and i["Group Key"] == j["Group Key"]:
                    return True
            case "Aggregate":
                if check_same_list(i['Relations'], j['Relations']) and i['Strategy'] == j['Strategy']:
                    if i['Strategy'] == 'Hashed' or i['Strategy'] == 'Sorted':
                        if i['Group Key'] == j['Group Key']:
                            return True
                    elif i['Strategy'] == 'Plain':
                        return True
            case _:
                if check_same_list(i['Relations'], j['Relations']): #if none of the operators match the switch statement, just check if involved relations are the same
                    return True

    return False

def check_same_list(list1, list2): #This function checks if 2 lists are the same regardless of order
    """
        Parameters:
            list1 (List): list of Relations
            list2 (List): list of Relations
        Returns:
            True/False (Bool) : Returns True if the 2 Lists are the same, else False
    """
    for x in set(list1 + list2):
        if list1.count(x) != list2.count(x):
            return False
    return True

def write_differences(st, node_list1, node_list2): #This function Identifies what changed in the new/evolved operators and display the differences
    """
        Parameters:
            st (streamlit): streamlit object
            node_list1 (List): node_list which contains the operators of the QEP
            node_list2 (List): node_list which contains the operators of the QEP
    """
    scans = ["Seq Scan", "Index Scan", "Bitmap Heap Scan", "Subquery Scan", "Function Scan", "Values Scan", "Index Only Scan"]
    joins = ["Hash Join", "Nested Loop", "Merge Join"]
                    
    new_steps = []
 
    found = False
    for i in node_list2:    #loop through evolved query
        found = False
        
        for j in node_list1:    #loop through original query to compare nodes and append changed/new nodes to new_steps list
            found = identify_same_nodes(i, j)
            
            if found == True:
                break
            
        if(not found):
            found = False
            new_steps.append(i)

    found = False
    for n in new_steps:
        found = False
        for m in node_list1:
            n_filter = "Nothing"
            m_filter = "Nothing"
            # print("----------------------TESTING ENTER------------------------", n['Node Type'], m['Node Type'])
            #condition to avoid error for unsupported operations like CTE scan
            # if n['Node Type'] in scans and m['Node Type'] in scans and ('Relation Name' not in n or 'Relation Name' not in m or 'Plans' in n or 'Plans' in m):
            #     if(n['Node Type'] != 'Bitmap Heap Scan' and m['Node Type'] != 'Bitmap Heap Scan'):
            #         continue
            if n['Node Type'] in scans and m['Node Type'] in scans and n['Relation Name'] == m['Relation Name']: #if the nodes are both scans on the same relation, find differences
                found = True
                if 'Filter' in n: n_filter = n['Filter']
                if 'Filter' in m: m_filter = m['Filter']
                if n['Node Type'] == m['Node Type']: #if they are the same type of scans e.g seq scan, but they differ by filter
                    st.write("In second query (Step",n['step'] ,"), relation ", n['Relation Name'], "is filtered by ", n_filter, "instead of being filterd by", m_filter, " in the first query.")
                    break
                else: # they differ by scan algorithm
                    st.write("In second query (Step",n['step'] ,"), relation ", n['Relation Name'], "is scanned using", n['Node Type'], "instead of ", m['Node Type'], "and filtered by ", n_filter, ".")
                    break

            if n['Node Type'] == 'Bitmap Index Scan' and m['Node Type'] == 'Bitmap Index Scan':
                if check_same_list(n['Relations'], m['Relations']) and n['Index Name'] == m['Index Name']:
                    found = True
                    st.write("In second query (Step",n['step'] ,"), Bitmap Index Scan was executed on", n['Index Name'], "with condition of", n['Index Cond'], "instead of", m['Index Cond'], "like in first query.")
                    break

            n_join = None
            m_join = None
            if 'Merg Cond' in n: n_join = n["Merg Cond"]
            if 'Hash Cond' in n: n_join = n["Hash Cond"]
            if 'Merg Cond' in m: m_join = m["Merg Cond"]
            if 'Hash Cond' in m: m_join = m["Hash Cond"]
            if n['Node Type'] in joins and m['Node Type'] in joins:   #Finding differences for join types
                if n['Node Type'] == 'Nested Loop' or m['Node Type'] == 'Nested Loop':
                    if check_same_list(n['Relations'],m['Relations']):
                        found = True
                        join_cond = n_join if n_join is not None else m_join
                        st.write("In second query (Step",n['step'] ,"), the condition ", join_cond , "is joined using ", n['Node Type'], "instead of being joined by", m['Node Type'], "like in the first query.")
                        break
                else:
                    if n_join == m_join:
                        found = True
                        st.write("In second query (Step",n['step'] ,"), the condition ", n_join , "is joined using ", n['Node Type'], "instead of being joined by", m['Node Type'], "like in the first query.")
                        break
                        
            if n['Node Type'] == 'Aggregate' and m['Node Type'] == 'Aggregate' and check_same_list(m['Relations'], n['Relations']):#m['Relations'] == n['Relations']:    #Finding differences for aggregate
                found = True
                if n['Strategy'] == 'Hashed' or n['Strategy'] == 'Sorted':
                    m_group_key = [None]
                    if 'Group Key' in m: m_group_key = m['Group Key']
                    st.write("In second query (Step",n['step'] ,"),", n['Strategy'], "aggregation was executed using keys :", str(n['Group Key']), "instead of executing by ", m['Strategy'], " aggregation using keys :", str(m_group_key), "like in the first query.")
                else:
                    st.write("In second query (Step",n['step'] ,"),", n['Strategy'], "aggregation was executed instead of executing by", m['Strategy'], "aggregation like in the first query.")
                break

            if n['Node Type'] == 'Group' and m['Node Type'] == 'Group' and check_same_list(n['Relations'], m['Relations']):#n['Relations'] == m['Relations']:    #Find differences for grouping
                found = True
                st.write("In second query (Step",n['step'] ,"), the grouping is performed using keys:", n['Group Key'], "instead of ", m['Group Key'], "like in the first query.")

            if n['Node Type'] == 'Sort' and m['Node Type'] == 'Sort' and check_same_list(n['Relations'], m['Relations']):#n['Relations'] == m['Relations']:
                found = True
                st.write("In second query (Step",n['step'] ,"), the sorting is performed using keys:", n['Sort Key'], "instead of ", m['Sort Key'], "like in the first query.")

        if found == False:
            st.write("New step in second query (Step",n['step'] ,"): ", get_exp(n))

def process_nodes(qep):
    """
        Parameters:
            qep: Representing a QEP of a query in JSON format
        Returns:
            graph_str (str): Dot string of created graph
            node_list (list): list of operations in query plan
    """
    # make lists of nodes and its sub plans
    node_list = []
    # declare empty dot string of graph 1
    graph_str = ''''''
    graph_str = graph_str + 'digraph {\n'

    # ========================== Create node lists with node types in QEP and create dot string for graph visualization ===============================
    q = deque([qep]) # get first subplan
    step = 1
    parentnum = 1
    root = 0
    while q:
        for i in range(len(q)): # iterate through all subplans
            node = q.popleft() 
            parent = str(node['Node Type']).replace(" ", "") # get node type of subplan
            if root == 0:
                graph_str = graph_str + str(step) + '[label="' + parent + '"]\n' # create dot string for graph visualization
                root = 1
                parentnum = step
                step = step + 1 # update graph index
            
            node_list.append(node) # append node type of subplan to node_list

            if "Plans" in node:
                
                for child in node['Plans']: # iterate through all childs of current node
                    graph_str = graph_str + str(step) + '[label="' + str(child['Node Type']).replace(" ", "") + '"]\n' # create dot string for graph visualization
                    graph_str = graph_str + str(parentnum) + '->' + str(step) + "\n" # create dot string for graph visualization
                    step = step + 1 # update graph index
                    q.append(child) # append child node to q
            parentnum = parentnum + 1
    graph_str = graph_str + '}' # close the dot string
    # ====================================================================================================================================================

    return graph_str, node_list