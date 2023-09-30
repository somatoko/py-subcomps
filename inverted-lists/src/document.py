

class Document:

    def __init__(self, id: int, content: str):
        self.id = id
        self.content = content

    def __str__(self):
        return f'{self.id} - {self.content}\n'

    __repr__ = __str__
