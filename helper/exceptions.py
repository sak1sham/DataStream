class ConnectionError(Exception):
    pass

class UnrecognizedFormat(Exception):
    pass

class DestinationNotFound(Exception):
    pass

class ProcessingError(Exception):
    pass

class InvalidArguments(Exception):
    pass

class SourceNotFound(Exception):
    pass

class MissingData(Exception):
    pass

class IncorrectMapping(Exception):
    pass

class APIRequestError(Exception):
    pass