# Troubleshooting RemoteDisconnected errors

When the uploader streams large files from Notion using a presigned URL, it relies on the `_PresignedStream` helper in `uploader/s3_downloader.py`. This helper wraps a shared `requests` session and yields chunks from `Response.iter_content`. If the remote server closes the socket before sending a complete HTTP response, `requests` raises `RemoteDisconnected` and, after exhausting the retry budget, the error propagates back to the Flask handler (as seen in the stack trace above). 【F:uploader/s3_downloader.py†L133-L205】

In the log snippet, the downloader successfully retrieved metadata and began streaming `The.LEGO.Movie.2014.2160p.UHD.BluRay.REMUX.HDR.HEVC.DTS-HD.MA.5.1-EPSiLON.mkv.part53`, but the origin `dr.makl.xyz` abruptly terminated the TLS connection without replying. Because the client never received an HTTP status line, the standard library raised `RemoteDisconnected("Remote end closed connection without response")`, which `requests` surfaces as a `ConnectionError` after the configured retries are exhausted.

This behavior is typically triggered by one of the following:

- The origin server enforces an idle timeout that is shorter than the time it takes to stream a chunk.
- Intermediate infrastructure (reverse proxy, CDN, or firewall) kills long-lived connections.
- The origin process crashes or rejects the request mid-transfer, often visible in accompanying `socket shutdown error: [Errno 9] Bad file descriptor` messages from the socket.io relay.

To mitigate these errors:

1. Verify the upstream server is healthy and capable of serving large ranges without timeouts.
2. Reduce the configured chunk size or concurrency so each request finishes sooner.
3. Ensure keep-alive and range requests are supported by the upstream.
4. If the issue persists, increase `_NUM_DOWNLOAD_ATTEMPTS` or add backoff to accommodate flaky origins.

Because the exception originates from the remote peer rather than our code, the fix usually involves stabilizing the upstream service or adjusting its timeouts.
