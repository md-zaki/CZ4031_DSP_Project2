from collections import deque
import psycopg2

from interface_tkinter import App
from explain import get_exp


def test_func(cursor, app):
    extract_qp = "EXPLAIN (ANALYZE false, SETTINGS true, FORMAT JSON) "
    query = app.get_query_input()
    try:
        cursor.execute(extract_qp + query)
    except Exception as e:
        app.display_error_message(str(e))

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
    
    # Print Query Execution Plan Tree from Postgres
    extract_qp = "EXPLAIN (COSTS FALSE, TIMING FALSE) "
    try:
        cursor.execute(extract_qp + query)
    except Exception as e:
        app.display_error_message(str(e))
    qep_list1 = cursor.fetchall()
    qep_display_text = "\n".join([i[0] for i in qep_list1])
    app.display_query_plan(qep_display_text)

    # Print Explanation
    exp_list = []
    count = 1
    for i in node_list:
        exp_list.append(str(count) + ". " + get_exp(i))
        count = count + 1
    exp_display_text = "\n\n".join([i for i in exp_list])
    app.display_explanation(exp_display_text)


if __name__ == "__main__":
    # connect to postgres
    conn = psycopg2.connect(database="postgres",
                            host="localhost",
                            user="postgres",
                            password="dspproject123",
                            port="5432")
    cursor = conn.cursor()

    app = App()
    app.q_submit_button.configure(command=lambda: test_func(cursor, app))  # Bind function to submit button

    app.mainloop()  # This must be the very last line
