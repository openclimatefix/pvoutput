""" Custom Exeception classes """
import requests


class BadStatusCode(Exception):
    """Bad status code excepction"""

    def __init__(self, response: requests.Response, message: str = ""):
        """Init"""
        self.response = response
        super(BadStatusCode, self).__init__(message)

    def __str__(self) -> str:
        """String method"""
        string = super(BadStatusCode, self).__str__()
        string += "Status code: {}; ".format(self.response.status_code)
        string += "Response content: {}; ".format(self.response.content)
        string += "Response headers: {}; ".format(self.response.headers)
        return string


class NoStatusFound(BadStatusCode):
    """Exeception for when no status code is found"""

    pass


class RateLimitExceeded(BadStatusCode):
    """Class for rate limit is exceeded"""

    pass
