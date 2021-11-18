import requests


class BadStatusCode(Exception):
    def __init__(self, response: requests.Response, message: str = ""):
        self.response = response
        super(BadStatusCode, self).__init__(message)

    def __str__(self) -> str:
        string = super(BadStatusCode, self).__str__()
        string += "Status code: {}; ".format(self.response.status_code)
        string += "Response content: {}; ".format(self.response.content)
        string += "Response headers: {}; ".format(self.response.headers)
        return string


class NoStatusFound(BadStatusCode):
    pass


class RateLimitExceeded(BadStatusCode):
    pass
