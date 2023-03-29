from types import MethodType

from streamz import Stream, Sink


@Stream.register_api()
class on_exception(Sink):
    def __init__(self, upstream: Stream, exception=Exception, **kwargs):
        super().__init__(upstream, **kwargs)

        original_upstream_update_method = upstream.update

        def _(upstream_self, x, who=None, metadata=None):
            try:
                return original_upstream_update_method(x, who, metadata)
            except exception as exc:
                # Pass down the branch started with this stream instead
                self._emit((x, exc), metadata)

        # Bind to upstream
        upstream.update = MethodType(_, upstream)

    def update(self, x, who=None, metadata=None):
        pass  # NO-OP
