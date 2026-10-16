"""Start Streamlit with a Windows event loop suited to its local server socket.

The default Proactor loop can log ``Accept failed on a socket`` when a local
client drops during accept. Select the Windows selector loop before Streamlit
creates its Tornado/asyncio server; this is scoped to the Streamlit process.
"""

from __future__ import annotations

import asyncio
import sys


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    from streamlit.web.cli import main as streamlit_main

    sys.argv = ["streamlit", "run", *sys.argv[1:]]
    streamlit_main()


if __name__ == "__main__":
    main()
