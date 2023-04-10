import streamlit as st
import graphviz
from collections import deque
import psycopg2
from explain import *

st.set_page_config(layout="wide")


# connection parameters for postgres
conn = psycopg2.connect(database="postgres",
                            host="localhost",
                            user="postgres",
                            password="dspproject123",
                            port="5432")
cursor = conn.cursor()
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
    query1 = st.text_input('Enter query 1')
    if query1 != "":
        cursor.execute(extract_qp + query1)
        # get query plan in JSON format
        qep1 = cursor.fetchall()[0][0][0].get("Plan")
        # make lists of nodes and its sub plans
        node_list = []

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
                
                node_list.append(node) # append node type of subplan to node_list

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
        node_list.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) " # update extract qp string
        cursor.execute(extract_qp + query1)
        qep_list1 = cursor.fetchall()
        
        # Print Explanation
        count = 1
        with st.expander("Description of Query 1:"):
            # st.subheader("Description: ")
            for i in node_list:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1
        
        
extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) " # update extract qp string
with col2:
    query2 = ""
    # User input query string
    query2 = st.text_input('Enter query 2')
    if query2 != "":
        cursor.execute(extract_qp + query2)
        # get query plan in JSON format
        qep2 = cursor.fetchall()[0][0][0].get("Plan")

        node_list = [] # make lists of nodes and its sub plans
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
                
                node_list.append(node) # append node type of subplan to node_list

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
        node_list.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) " # update extract qp string
        cursor.execute(extract_qp + query2)
        qep_list2 = cursor.fetchall()

        # Print Explanation
        count = 1
        with st.expander("Description of Query 2:"):
            for i in node_list:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1

# ================== Get differences in labels between the 2 graphs ======================
try:
    missing1, missing2 = compare_graph_labels(graph_str1, graph_str2)

    for i in missing1:
        graph_str2 = highlight_node(graph_str2, i) # highlight differences in red

    for i in missing2:
        graph_str1 = highlight_node(graph_str1,i) # highlight differences in red
except:
    st.error("Please key in Both Queries")
# =========================================================================================


# =========================== Display Graph =====================================
with col1:
    try:
        with st.expander("QEP Tree of Query 1"):
                # st.subheader("Query Execution Plan Tree from Postgres")
                print("Q1")
                print(graph_str1)
                # # for i in qep_list1:
                # #     st.write(i[0])
                st.graphviz_chart(graph_str1)
    except:
        st.error("Please key in Original Query")
with col2:
    try:
        with st.expander("QEP Tree of Query 2"):
                # st.subheader("Query Execution Plan Tree from Postgres")
                print("Q2")
                print(graph_str2)
                st.graphviz_chart(graph_str2)
    except:
        st.error("Please key in Evolved Query")

# =============================================================================

st.header("Differences")
st.write("Difference description is here")

