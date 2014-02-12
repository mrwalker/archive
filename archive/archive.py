from jinja2 import Environment, PackageLoader

from workflow import Workflow

class Archive(Workflow):
  '''
  An Archive is a container for the queries that make up your Hive.
  '''

  def __init__(self, package, hive, templates = 'templates'):
    self.package = package
    self.templates = templates
    self.env = Environment(loader = PackageLoader(
      self.package,
      self.templates
    ))

    self.hive = hive
    self.queries = {}

  def lookup(self, query_name):
    return self.queries[query_name]

  def add(self, query):
    if query.name in self.queries:
      raise RuntimeError("Queries must have unique names; Archive already contains query '%s'" % query.name)

    self.validate(query)

    self.queries[query.name] = query
    query.archive = self

    return query

  def validate(self, query):
    for i in query.inputs:
      self._validate(i)

  def _validate(self, query):
    if query.name not in self.queries:
      raise RuntimeError("Query '%s' not in Archive; did you forget to add it?\nCurrently archived: %s" % (query.name, self.queries))

    for i in query.inputs:
      self._validate(i)

  def graph(self):
    context = {
      'offset': 0,
      'references': {},
    }

    graph_str = 'Archive: %s' % self.package
    for r in self.queries.values():
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
      'queries': {
        'unique_queries': set(),
        'references': {},
      }
    }
    for r in self.queries.values():
      r._stats(stats)

    stats['archive']['databases'] = len(stats['databases']['unique_databases'])
    stats['archive']['queries'] = len(stats['queries']['unique_queries'])
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
    for r in self.queries.values():
      all_create_hql += r._create_all_hql(created)

    return all_create_hql
