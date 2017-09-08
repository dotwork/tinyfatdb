########################################################################
class TinyFatModel(dict):
    """
    Base/default model class.
    Holds a single entry from a TinyFatDB table and enables adding methods
    to the entry data/dictionary in a "fat models" style.
    Also adds the 'eid' as a key/value pair on the element.
    """
    eid = None

    ###################################################################
    def __init__(self, element, **kwargs):
        """
        Load data from database into the current model instance.
        :param data: Element object from TinyDB
        """
        super(TinyFatModel, self).__init__(element, **kwargs)
        self.eid = self["eid"] = element.eid
