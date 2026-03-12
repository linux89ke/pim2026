import streamlit as st
import streamlit.components.v1 as components

st.title("Bridge Attribute Debugger")

# Render input with a unique placeholder
val = st.text_input(
    "jtbridge",
    value="",
    placeholder="JTBRIDGE_UNIQUE_DO_NOT_USE",
    label_visibility="collapsed",
    key="debug_bridge"
)

# JS that scans ALL inputs and reports every attribute found on each
components.html("""
<script>
setTimeout(function() {
  const inputs = window.parent.document.querySelectorAll('input[type="text"]');
  const report = [];
  inputs.forEach(function(inp, i) {
    const attrs = {};
    for (const a of inp.attributes) {
      attrs[a.name] = a.value;
    }
    // Also check non-attribute properties
    attrs['__placeholder_prop'] = inp.placeholder;
    attrs['__value_prop']       = inp.value;
    report.push({ index: i, attrs });
  });
  document.getElementById('out').textContent = JSON.stringify(report, null, 2);
}, 800);
</script>
<pre id="out" style="font-size:11px;background:#f5f5f5;padding:12px;border-radius:6px;overflow:auto;max-height:500px;">
  Scanning…
</pre>
""", height=550)

st.caption("Wait 1 second — the box above will show every attribute on every text input in the page.")
st.caption(f"Current bridge value: `{repr(val)}`")
