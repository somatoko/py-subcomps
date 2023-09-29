

class Document:
    oid = 0

    def __init__(self, abs_path, _dict=None):
        if _dict is not None:
            self.id = _dict['id']
            self.path = _dict['path']
            self.name = _dict['name']
            self.suffix = _dict['suffix']
        else:
            p = Path(abs_path)
            self.__class__.oid += 1
            self.id = self.__class__.oid
            self.path = abs_path
            self.name = self.__normalize_name(p.stem)
            self.suffix = p.suffix

    def __normalize_name(self, name):
        return (name
                .replace('.', ' ')
                .replace('-', ' ')
                .replace('_', ' ')
                .replace('[', '')
                .replace(']', '')
                .replace('  ', ' ')
                )

    def __str__(self):
        return f'{self.id} - {self.name}\ntype: {self.suffix}\npath: {self.path}'

    __repr__ = __str__

    @property
    def __dict__(self):
        return dict(
            id=self.id,
            name=self.name,
            suffix=self.suffix,
            path=str(self.path.relative_to(BOOKS_ROOT)),
        )
