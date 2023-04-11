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
    try:
        # query1 = st.text_input('Enter Original Query')
        query1 = st.text_area('Enter Original Query:')
        if query1 != "":
            query1 = " ".join(query1.splitlines())  # ensures multi-line text is properly joined
            cursor.execute(extract_qp + query1)
            # get query plan in JSON format
            qep1 = cursor.fetchall()[0][0][0].get("Plan")

            #process nodes to get node_list of all operations and to get dot string of created graph
            graph_str1, node_list1 = process_nodes(qep1)
            
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
    except Exception as e:
        st.error(e)
        
        
extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) " # update extract qp string
with col2:
    query2 = ""
    # User input query string
    try:
        # query2 = st.text_input('Enter Evolved Query')
        query2 = st.text_area('Enter Evolved Query:')
        if query2 != "":
            query2 = " ".join(query2.splitlines())  # ensures multi-line text is properly joined
            cursor.execute(extract_qp + query2)
            # get query plan in JSON format
            qep2 = cursor.fetchall()[0][0][0].get("Plan")

            node_list2 = [] # make lists of nodes and its sub plans
            graph_str2 = ''''''
            graph_str2 = graph_str2 + 'digraph {'

            #process nodes to get node_list of all operations and to get dot string of created graph
            graph_str2,node_list2 = process_nodes(qep2)
        
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
    except Exception as e:
        st.error(e)

# ================== Get differences in labels between the 2 graphs ======================
try:
    missing1, missing2 = compare_graph_labels(graph_str1, graph_str2)

    for i in missing1:
        graph_str2 = highlight_node(graph_str2, i) # highlight differences in red

    for i in missing2:
        graph_str1 = highlight_node(graph_str1,i) # highlight differences in red
    changes_query = query_diff(query1, query2)
except:
    st.error("Please ensure you have entered **BOTH** queries and they are valid")
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
        pass
        # st.error("Please key in Original Query")
with col2:
    try:
        with st.expander("QEP Tree of Evolved Query"):
                # st.subheader("Query Execution Plan Tree from Postgres")
                # print("Q2")
                # print(graph_str2)
                st.graphviz_chart(graph_str2)
    except:
        pass
        # st.error("Please key in Evolved Query")

# =============================================================================


with st.expander("How the Query Execution Plans have evolved:"):
        st.write(changes_query)
        try:
            diff_str = qep_diff_exp(missing1, missing2)
            st.subheader('Overall differences')
            st.write(diff_str)
            
        except:
            st.success('Both queries share the same operation types')
        
        try:
            st.subheader("Step by step differences")
            write_differences(st, node_list1, node_list2)
        except:
            st.error("No Differences")
    


