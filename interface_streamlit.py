import streamlit as st
import graphviz
from collections import deque
import psycopg2
from explain import *
st.set_page_config(layout="wide")
st.title("QEP Visualizer and Comparison")
conn = psycopg2.connect(database="postgres",
                            host="localhost",
                            user="postgres",
                            password="dspproject123",
                            port="5432")
cursor = conn.cursor()

extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) "
col1, col2 = st.columns(2, gap="small")
with col1:
    nochild = 0
    query1 = ""
    # Specify queries to be explained here
    query1 = st.text_input('Enter query 1')
    if query1 != "":
        cursor.execute(extract_qp + query1)
        # get query plan in JSON format
        qep1 = cursor.fetchall()[0][0][0].get("Plan")
        # make lists of nodes and its sub plans
        node_list = []
        graph_str1 = ''''''
        graph_str1 = graph_str1 + 'digraph {\n'
        q = deque([qep1])
        step = 1
        parentnum = 1
        root = 0
        while q:
            for i in range(len(q)):
                node = q.popleft()
                parent = str(node['Node Type']).replace(" ", "")
                if root == 0:
                    graph_str1 = graph_str1 + str(step) + '[label="' + parent + '"]\n'
                    root = 1
                    parentnum = step
                    step = step + 1
                
                node_list.append(node)
                if "Plans" in node:
                    
                    for child in node['Plans']:
                        graph_str1 = graph_str1 + str(step) + '[label="' + str(child['Node Type']).replace(" ", "") + '"]\n'
                        graph_str1 = graph_str1 + str(parentnum) + '->' + str(step) + "\n"
                        step = step + 1
                        # graph_str = graph_str + parent + " -> " + str(child['Node Type'] + str(step)).replace(" ", "") + "\n"
                        q.append(child)
                parentnum = parentnum + 1

        graph_str1 = graph_str1 + '}'
        # Reverse the list
        node_list.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) "
        cursor.execute(extract_qp + query1)
        qep_list1 = cursor.fetchall()
        
        # Print Explanation
        count = 1
        with st.expander("Description of Query 1:"):
            # st.subheader("Description: ")
            for i in node_list:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1
        
        
extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) "
with col2:
    query2 = ""
    # Specify queries to be explained here
    query2 = st.text_input('Enter query 2')
    if query2 != "":
        cursor.execute(extract_qp + query2)
        # get query plan in JSON format
        qep2 = cursor.fetchall()[0][0][0].get("Plan")

        node_list = []
        graph_str2 = ''''''
        graph_str2 = graph_str2 + 'digraph {'
        q = deque([qep2])
        step = 1
        parentnum = 1
        root = 0
        while q:
            for i in range(len(q)):
                node = q.popleft()
                parent = str(node['Node Type']).replace(" ", "")
                if root == 0:
                    graph_str2 = graph_str2 + str(step) + '[label="' + parent + '"]\n'
                    root = 1
                    parentnum = step
                    step = step + 1
                
                node_list.append(node)
                if "Plans" in node:
                    
                    for child in node['Plans']:
                        graph_str2 = graph_str2 + str(step) + '[label="' + str(child['Node Type']).replace(" ", "") + '"]\n'
                        graph_str2 = graph_str2 + str(parentnum) + '->' + str(step) + "\n"
                        step = step + 1
                        # graph_str = graph_str + parent + " -> " + str(child['Node Type'] + str(step)).replace(" ", "") + "\n"
                        q.append(child)
                parentnum = parentnum + 1
                
        graph_str2 = graph_str2 + '}'
        # Reverse the list
        # Reverse the list
        node_list.reverse()

        extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) "
        cursor.execute(extract_qp + query2)
        qep_list2 = cursor.fetchall()
        # Print Explanation
        count = 1
        with st.expander("Description of Query 2:"):
            # st.subheader("Description: ")
            for i in node_list:
                st.write(str(count) + ". " + get_exp(i))
                count = count + 1

missing1, missing2 = compare_graph_labels(graph_str1, graph_str2)

for i in missing1:
    graph_str2 = highlight_node(graph_str2, i)

for i in missing2:
    graph_str1 = highlight_node(graph_str1,i)
with col1:
    with st.expander("QEP Tree of Query 1"):
            # st.subheader("Query Execution Plan Tree from Postgres")
            print("Q1")
            print(graph_str1)
            # # for i in qep_list1:
            # #     st.write(i[0])
            st.graphviz_chart(graph_str1)
with col2:
    with st.expander("QEP Tree of Query 2"):
            # st.subheader("Query Execution Plan Tree from Postgres")
            print("Q2")
            print(graph_str2)
            st.graphviz_chart(graph_str2)

st.header("Differences")
st.write("Difference description is here")


# import graphviz
# graph = '''
#     digraph {{
#         {e1} [color ="red"]
#         {e1} -> {e2}
#     }}
# '''.format(e1 = 'element1', e2 = 'element2')

# st.graphviz_chart(graph)