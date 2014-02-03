from jinja2 import Environment, PackageLoader

class Archive:
  '''
  An Archive is a container for the relations that make up your Hive.
  '''

  def __init__(self, package, hive, templates = 'templates'):
    self.package = package
    self.templates = templates
    self.env = Environment(loader = PackageLoader(
      self.package,
      self.templates
    ))

    self.hive = hive
    self.relations = {}

  def add(self, relation):
    if relation.name in self.relations:
      raise RuntimeError("Relations must have unique names; Archive already contains relation '%s'" % relation.name)

    self.validate(relation)

    self.relations[relation.name] = relation
    relation.env = self.env
    relation.hive = self.hive

    return relation

  def validate(self, relation):
    for i in relation.inputs:
      self._validate(i)

  def _validate(self, relation):
    if relation.name not in self.relations:
      raise RuntimeError("Relation '%s' not in Archive; did you forget to add it?\nCurrently archived: %s" % (relation.name, self.relations))

    for i in relation.inputs:
      self._validate(i)
