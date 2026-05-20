import streamlit as st
html_str = "<html><body>Hello</body></html>"
try:
    st.iframe(html_str, height=100)
    print("SUCCESS")
except Exception as e:
    print(f"FAILED: {e}")
