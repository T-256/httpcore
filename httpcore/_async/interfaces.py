import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Union

from .._models import (
    URL,
    Extensions,
    HeaderTypes,
    Origin,
    Request,
    Response,
    enforce_bytes,
    enforce_headers,
    enforce_url,
    include_request_headers,
)


class AsyncRequestInterface:
    async def _cleanup(self, request: Request) -> None:
        _, exc, _ = sys.exc_info()
        if not exc:
            return

        s = [status for status in self._requests if status.request is request]
        if s:
            with AsyncShieldCancellation():
                status = s[-1]
                if status.connection is None:
                    # If we timeout here, or if the task is cancelled, then make
                    # sure to remove the request from the queue before bubbling
                    # up the exception.
                    async with self._pool_lock:
                        # Ensure only remove when task exists.
                        if status in self._requests:
                            self._requests.remove(status)
                else:
                    # TODO: refactor `RuntimeError` as `httpcore` exception class.
                    if not isinstance(exc, RuntimeError):
                        exc = await connection._cleanup(request, exc)
                    await self.response_closed(status)
        raise exc

    async def request(
        self,
        method: Union[bytes, str],
        url: Union[URL, bytes, str],
        *,
        headers: HeaderTypes = None,
        content: Union[bytes, AsyncIterator[bytes], None] = None,
        extensions: Optional[Extensions] = None,
    ) -> Response:
        # Strict type checking on our parameters.
        method = enforce_bytes(method, name="method")
        url = enforce_url(url, name="url")
        headers = enforce_headers(headers, name="headers")

        # Include Host header, and optionally Content-Length or Transfer-Encoding.
        headers = include_request_headers(headers, url=url, content=content)

        request = Request(
            method=method,
            url=url,
            headers=headers,
            content=content,
            extensions=extensions,
        )
        try:
            response = await self.handle_async_request(request)
        finally:
            with AsyncShieldCancellation():
                await self._cleanup(request)
        try:
            await response.aread()
        finally:
            with AsyncShieldCancellation():
                await response.aclose()
        return response

    @asynccontextmanager
    async def stream(
        self,
        method: Union[bytes, str],
        url: Union[URL, bytes, str],
        *,
        headers: HeaderTypes = None,
        content: Union[bytes, AsyncIterator[bytes], None] = None,
        extensions: Optional[Extensions] = None,
    ) -> AsyncIterator[Response]:
        # Strict type checking on our parameters.
        method = enforce_bytes(method, name="method")
        url = enforce_url(url, name="url")
        headers = enforce_headers(headers, name="headers")

        # Include Host header, and optionally Content-Length or Transfer-Encoding.
        headers = include_request_headers(headers, url=url, content=content)

        request = Request(
            method=method,
            url=url,
            headers=headers,
            content=content,
            extensions=extensions,
        )
        try:
            response = await self.handle_async_request(request)
        finally:
            with AsyncShieldCancellation():
                await self._cleanup(request)
        try:
            yield response
        finally:
            with AsyncShieldCancellation():
                await response.aclose()

    async def handle_async_request(self, request: Request) -> Response:
        raise NotImplementedError()  # pragma: nocover


class AsyncConnectionInterface(AsyncRequestInterface):
    async def aclose(self) -> None:
        raise NotImplementedError()  # pragma: nocover

    def info(self) -> str:
        raise NotImplementedError()  # pragma: nocover

    def can_handle_request(self, origin: Origin) -> bool:
        raise NotImplementedError()  # pragma: nocover

    def is_available(self) -> bool:
        """
        Return `True` if the connection is currently able to accept an
        outgoing request.

        An HTTP/1.1 connection will only be available if it is currently idle.

        An HTTP/2 connection will be available so long as the stream ID space is
        not yet exhausted, and the connection is not in an error state.

        While the connection is being established we may not yet know if it is going
        to result in an HTTP/1.1 or HTTP/2 connection. The connection should be
        treated as being available, but might ultimately raise `NewConnectionRequired`
        required exceptions if multiple requests are attempted over a connection
        that ends up being established as HTTP/1.1.
        """
        raise NotImplementedError()  # pragma: nocover

    def has_expired(self) -> bool:
        """
        Return `True` if the connection is in a state where it should be closed.

        This either means that the connection is idle and it has passed the
        expiry time on its keep-alive, or that server has sent an EOF.
        """
        raise NotImplementedError()  # pragma: nocover

    def is_idle(self) -> bool:
        """
        Return `True` if the connection is currently idle.
        """
        raise NotImplementedError()  # pragma: nocover

    def is_closed(self) -> bool:
        """
        Return `True` if the connection has been closed.

        Used when a response is closed to determine if the connection may be
        returned to the connection pool or not.
        """
        raise NotImplementedError()  # pragma: nocover

    def _has_sub_connection(self) -> bool:
        return hasattr(self, "_connection")

    async def _cleanup(self, request: Request, exc: BaseException) -> BaseException:
        """
        Return `BaseException`.

        Used for iterating over nested interfaces when cleanup.
        """
        if hasattr(self, "_connection"):
            if self._connection is None:
                # couldn't connect and raised exception.
                self._connect_failed = True
            else:
                assert isinstance(self._connection, AsyncConnectionInterface)
                return await self._connection._cleanup(request, exc)

        return exc
