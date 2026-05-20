import streamlit as st
import sys
html_str = "<html><body><h1>Hello</h1></body></html>"
dg = st.iframe(html_str, height=100)
# Streamlit doesn't return HTML, but we can inspect the st._main.dg or something
