import dataclasses

def modify_init(cls):
    old_init = cls.__init__
    field_names = [f.name for f in dataclasses.fields(cls)]

    def __init__(self, **kwargs):
        for_old_init = {}
        for_new_init = {}
        for k, v in kwargs.items():
            if k in field_names:
                for_old_init[k] = v
            else:
                for_new_init[k] = v
        old_init(self, **for_old_init)
        for k, v in for_new_init.items():
            setattr(self, k, v)

    cls.__init__ = __init__
    return cls

