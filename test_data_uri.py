import streamlit as st
import urllib.parse
html_content = "<html><body style='background:red;'>Hello</body></html>"
src = "data:text/html;charset=utf-8," + urllib.parse.quote(html_content)
st.iframe(src, height=750)
