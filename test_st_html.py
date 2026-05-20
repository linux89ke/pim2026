import streamlit as st
import html

html_content = "<html><body>Hello</body></html>"
iframe_code = f'<iframe srcdoc="{html.escape(html_content)}" style="width: 100%; height: 750px; border: none;" scrolling="yes"></iframe>'
st.html(iframe_code)
