"""Module conaining useful custom streamz nodes"""
from types import MethodType
from logging import Logger

from streamz import Stream, Sink


@Stream.register_api()
class on_exception(Sink):  # pylint: disable=invalid-name
    """Monkey patching to generically catch exceptions within a pipeline"""

    def __init__(
        self, upstream: Stream, exception=Exception, logger: Logger = None, **kwargs
    ):
        super().__init__(upstream, **kwargs)

        original_upstream_update_method = upstream.update

        def _(_, payload, who=None, metadata=None):
            try:
                return original_upstream_update_method(payload, who, metadata)
            except exception as exc:  # pylint: disable=broad-exception-caught
                # Pass down the branch started with this stream instead
                if logger:
                    logger.exception("Caught the following exception in %s", upstream)
                return self._emit((payload, exc), metadata)

        # Bind to upstream
        upstream.update = MethodType(_, upstream)

    def update(self, x, who=None, metadata=None):
        pass  # NO-OP
