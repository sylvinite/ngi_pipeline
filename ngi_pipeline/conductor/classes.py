## TODO make these hashable (__hash__, __eq__) in some meaningful way
## TODO Add path checking, os.path.abspath / os.path.exists
class NGIObject(object):
    def __init__(self, name, dirname, subitem_type):
        self.name = name
        self.dirname = dirname
        self._subitems = {}
        self._subitem_type = subitem_type

    def _add_subitem(self, name, dirname):
        # Only add a new item if the same item doesn't already exist
        try:
            subitem = self._subitems[name]
        except KeyError:
            subitem = self._subitems[name] = self._subitem_type(name, dirname)
        return subitem

    def __iter__(self):
        return iter(self._subitems.values())

    def __unicode__(self):
        return self.name

    def __str__(self):
        return self.__unicode__()

    def __repr__(self):
        return "{}: \"{}\"".format(type(self), self.name)


class NGIProject(NGIObject):
    def __init__(self, name, dirname, base_path):
        self.base_path = base_path
        super(NGIProject, self).__init__(name, dirname, subitem_type=NGISample)
        self.samples = self._subitems
        self.add_sample = self._add_subitem
        self.command_lines = []


class NGISample(NGIObject):
    def __init__(self, *args, **kwargs):
        super(NGISample, self).__init__(subitem_type=NGIFCID, *args, **kwargs)
        self.fcids = self._subitems
        self.add_fcid = self._add_subitem


class NGIFCID(NGIObject):
    def __init__(self, *args, **kwargs):
        super(NGIFCID, self).__init__(subitem_type=None, *args, **kwargs)
        self.fastqs = self._subitems = []
        ## Not working
        #delattr(self, "_add_subitem")

    def __iter__(self):
        return iter(self._subitems)

    def add_fastq_files(self, fastq):
        if type(fastq) == list:
            self._subitems.extend(fastq)
        elif type(fastq) == str:
            self._subitems.append(fastq)
        else:
            raise TypeError("Fastq files must be passed as a list or a string: " \
                            "got \"{}\"".format(fastq))
