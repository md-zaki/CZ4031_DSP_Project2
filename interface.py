import streamlit as st
import graphviz

graph = '''
    digraph {{
        {e1} [color ="red"]
        {e1} -> {e2}
    }}
'''.format(e1 = 'element1', e2 = 'element2')

st.graphviz_chart(graph)