class ConnectorError(Exception):
    pass


class ConnectorTimeoutError(ConnectorError, TimeoutError):
    pass


class ConnectorProxyTimeoutError(ConnectorTimeoutError):
    pass
