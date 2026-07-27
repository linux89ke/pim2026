"""
Entry point for the validation API server.

Runs uvicorn on the Windows *selector* event loop instead of the default
proactor loop. The proactor loop spams the console with

    Exception in callback _ProactorBasePipeTransport._call_connection_lost
    ...
    self._sock.shutdown(socket.SHUT_RDWR)

whenever a client (e.g. the Streamlit app abandoning a /status poll
mid-rerun) drops an HTTP connection abruptly — a known, harmless CPython
issue on Windows, but noisy. The selector loop handles lost connections
cleanly. The policy must be set before uvicorn creates its event loop,
which is why this wrapper exists instead of `py -m uvicorn api:app`.
"""
import asyncio
import sys

import uvicorn

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000)
