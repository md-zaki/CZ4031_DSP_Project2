import streamlit as st
import graphviz
from collections import deque
import psycopg2
import argparse
from explain import *

parser = argparse.ArgumentParser()

parser.add_argument('--db')
parser.add_argument('--host')
parser.add_argument('--user')
parser.add_argument('--pwd')
args = parser.parse_args()
st.set_page_config(layout="wide")

# connection parameters for postgres
conn = psycopg2.connect(database=args.db,
                            host=args.host,
                            user=args.user,
                            password=args.pwd,
                            port="5432")
cursor = conn.cursor()


# ==================== Display table names and column names at the left side ====================
with st.sidebar:
    st.header("Database schema")
    st.write(f"**(current DB: {conn.info.dbname})**")
    # Get table names from current database
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    table_names = sorted([row[0] for row in cursor])
    for table_name in table_names:
        with st.expander(f"**{table_name}**"):
            # Get column names and data types from current table
            cursor.execute(f"SELECT column_name, data_type, ordinal_position FROM information_schema.columns \
                           WHERE table_name='{table_name}' ORDER BY ordinal_position")
            column_tuples = [row for row in cursor]  # each "row" will be a (col_name, col_type, col_position) tuple
            for tuple in column_tuples:
                st.write(f"&nbsp;&nbsp;&nbsp;&nbsp;{tuple[0]} ({tuple[1]})")  # "&nbsp;" is space in markdown
# ===============================================================================================


# declare postgres extract qp string
extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) "

# main title
st.title("QEP Visualizer and Comparison")

# declare column variables
col1, col2 = st.columns(2, gap="small")

# UI in column 1
with col1:
    query1 = ""
    # User input query string
    query1 = st.text_input('Enter Original Query')
    if query1 != "":
        cursor.execute(extract_qp + query1)
        # get query plan in JSON format
        qep1 = cursor.fetchall()[0][0][0].get("Plan")
        # make lists of nodes and its sub plans
        node_list1 = []

        # declare empty dot string of graph 1
        graph_str1 = ''''''
        graph_str1 = graph_str1 + 'digraph {\n'

        # ========================== Create node lists with node types in QEP and create dot string for graph visualization ===============================
        q = deque([qep1]) # get first subplan
        step = 1
        parentnum = 1
        root = 0
        while q:
            for i in range(len(q)): # iterate through all subplans
                node = q.popleft() 
                parent = str(node['Node Type']).replace(" ", "") # get node type of subplan
                if root == 0:
                    graph_str1 = graph_str1 + str(step) + '[label="' + parent + '"]\n' # create dot string for graph visualization
                    root = 1
                    parentnum = step
                    step = step + 1 # update graph index
                
                node_list1.append(node) # append node type of subplan to node_list

                if "Plans" in node:
                    
                    for child in node['Plans']: # iterate through all childs of current node
                        graph_str1 = graph_str1 + str(step) + '[label="' + str(child['Node Type']).replace(" ", "") + '"]\n' # create dot string for graph visualization
                        graph_str1 = graph_str1 + str(parentnum) + '->' + str(step) + "\n" # create dot string for graph visualization
                        step = step + 1 # update graph index
                        q.append(child) # append child node to q
                parentnum = parentnum + 1
        graph_str1 = graph_str1 + '}' # close the dot string

        # ====================================================================================================================================================
        # Reverse the list
        node_list1.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) " # update extract qp string
        cursor.execute(extract_qp + query1)
        qep_list1 = cursor.fetchall()
        
        node_list1 = add_relation_details(node_list1)
        # Print Explanation
        count = 1
        with st.expander("Description of Original Query:"):
            # st.subheader("Description: ")
            for i in node_list1:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1
        
        
extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) " # update extract qp string
with col2:
    query2 = ""
    # User input query string
    query2 = st.text_input('Enter Evolved Query')
    if query2 != "":
        cursor.execute(extract_qp + query2)
        # get query plan in JSON format
        qep2 = cursor.fetchall()[0][0][0].get("Plan")

        node_list2 = [] # make lists of nodes and its sub plans
        graph_str2 = ''''''
        graph_str2 = graph_str2 + 'digraph {'

        # ========================== Create node lists with node types in QEP and create dot string for graph visualization ===============================
        q = deque([qep2])
        step = 1
        parentnum = 1
        root = 0
        while q:
            for i in range(len(q)): # iterate through all subplans
                node = q.popleft()
                parent = str(node['Node Type']).replace(" ", "") # get node type of subplan
                if root == 0:
                    graph_str2 = graph_str2 + str(step) + '[label="' + parent + '"]\n' # create dot string for graph visualization
                    root = 1
                    parentnum = step
                    step = step + 1
                
                node_list2.append(node) # append node type of subplan to node_list

                if "Plans" in node:
                    
                    for child in node['Plans']: # iterate through all childs of current node
                        graph_str2 = graph_str2 + str(step) + '[label="' + str(child['Node Type']).replace(" ", "") + '"]\n' # create dot string for graph visualization
                        graph_str2 = graph_str2 + str(parentnum) + '->' + str(step) + "\n" # create dot string for graph visualization
                        step = step + 1
                        q.append(child) # append child node to q
                parentnum = parentnum + 1
                
        graph_str2 = graph_str2 + '}' # close the dot string
        # ====================================================================================================================================================
        # Reverse the list
        node_list2.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) " # update extract qp string
        cursor.execute(extract_qp + query2)
        qep_list2 = cursor.fetchall()

        node_list2 = add_relation_details(node_list2)
        # Print Explanation
        count = 1
        with st.expander("Description of Evolved Query:"):
            for i in node_list2:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1

# ================== Get differences in labels between the 2 graphs ======================
try:
    missing1, missing2 = compare_graph_labels(graph_str1, graph_str2)

    for i in missing1:
        graph_str2 = highlight_node(graph_str2, i) # highlight differences in red

    for i in missing2:
        graph_str1 = highlight_node(graph_str1,i) # highlight differences in red
    changes_query = query_diff(query1, query2)
except:
    st.error("Please key in Both Queries")
# =========================================================================================


# =========================== Display Graph =====================================
with col1:
    try:
        with st.expander("QEP Tree of Original Query"):
                # st.subheader("Query Execution Plan Tree from Postgres")
                # print("Q1")
                # print(graph_str1)
                # # for i in qep_list1:
                # #     st.write(i[0])
                st.graphviz_chart(graph_str1)
    except:
        st.error("Please key in Original Query")
with col2:
    try:
        with st.expander("QEP Tree of Evolved Query"):
                # st.subheader("Query Execution Plan Tree from Postgres")
                # print("Q2")
                # print(graph_str2)
                st.graphviz_chart(graph_str2)
    except:
        st.error("Please key in Evolved Query")

# =============================================================================


with st.expander("How the Query Execution Plans have evolved:"):
    
        try:
            diff_str = qep_diff_exp(missing1, missing2)
            st.write(diff_str)
        except:
            st.error('Both queries share the same operation types')
        st.write(changes_query)
        st.write("------------------------------------------------------Step by step differences------------------------------------------------------")
        write_differences(st, node_list1, node_list2)
    


