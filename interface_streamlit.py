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
    query1 = ""
    # Specify queries to be explained here
    query1 = st.text_input('Enter query 1')
    if query1 != "":
        cursor.execute(extract_qp + query1)
        # get query plan in JSON format
        qep1 = cursor.fetchall()[0][0][0].get("Plan")
        # make lists of nodes and its sub plans
        node_list = []
        graph_str = ''''''
        graph_str = graph_str + 'digraph {'
        q = deque([qep1])
        step = 1
        while q:
            for i in range(len(q)):
                node = q.popleft()
                parent = str(node['Node Type'] + str(step)).replace(" ", "")
                node_list.append(node)
                if "Plans" in node:
                    step = step + 1
                    for child in node['Plans']:
                        graph_str = graph_str + parent + " -> " + str(child['Node Type'] + str(step)).replace(" ", "") + "\n"
                        q.append(child)
                
        graph_str = graph_str + '}'
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
        with st.expander("QEP Tree of Query 1"):
            # st.subheader("Query Execution Plan Tree from Postgres")
            st.graphviz_chart(graph_str)
        
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
        graph_str = ''''''
        graph_str = graph_str + 'digraph {'
        q = deque([qep2])
        step = 1
        while q:
            for i in range(len(q)):
                node = q.popleft()
                parent = str(node['Node Type'] + str(step)).replace(" ", "")
                node_list.append(node)
                if "Plans" in node:
                    step = step + 1
                    for child in node['Plans']:
                        graph_str = graph_str + parent + " -> " + str(child['Node Type'] + str(step)).replace(" ", "") + "\n"
                        q.append(child)
                
        graph_str = graph_str + '}'
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
        with st.expander("QEP Tree of Query 2"):
            # st.subheader("Query Execution Plan Tree from Postgres")
            st.graphviz_chart(graph_str)
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