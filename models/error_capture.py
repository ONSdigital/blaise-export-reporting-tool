import json


class BertException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code
        print(self.message)

    def json(self):
        return json.dumps({"error": self.message})


class RowNotFound(Exception):
    pass
