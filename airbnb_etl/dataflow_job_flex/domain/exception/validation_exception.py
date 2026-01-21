from typing import List

class AirbnbValidationException(Exception):

    def __init__(self, errors):
        self.errors = errors
        super().__init__(errors)

