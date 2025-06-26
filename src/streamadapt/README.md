# streamadapt

Robust, type-safe adaptation for binary and text streams in Python.

## Features

- `ensure_stream` – single entry point for binary and text streams
- Detects mode using `read(0)`
- Optionally wraps non-seekable streams in an in-memory wrapper
- Works with sockets, pipes, `BytesIO`, `StringIO`, and files
