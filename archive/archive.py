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

  def lookup(self, relation_name):
    return self.relations[relation_name]

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

  def graph(self):
    context = {
      'offset': 0,
      'references': {},
    }

    graph_str = 'Archive: %s' % self.package
    for r in self.relations.values():
      graph_str += '\n%s' % r._graph(context)

    return graph_str

  def stats(self):
    stats = {
      'archive': {
        'depth': 0,
        'current_depth': 0,
      },
      'databases': {
        'unique_databases': set(),
        'references': {},
      },
      'relations': {
        'unique_relations': set(),
        'references': {},
      }
    }
    for r in self.relations.values():
      r._stats(stats)

    stats['archive']['databases'] = len(stats['databases']['unique_databases'])
    stats['archive']['relations'] = len(stats['relations']['unique_relations'])
    stats['archive'].pop('current_depth', None)
    return stats

  def create_all(self):
    query = self.create_all_hql()
    hive_job = self.hive.run_async(query)
    return hive_job

  def create_all_hql(self):
    # Used only to set view_or_table
    self.graph()
    created = []

    all_create_hql = '-- Archive-generated HQL for Archive: %s\n' % self.package
    for r in self.relations.values():
      all_create_hql += r._create_all_hql(created)

    return all_create_hql
