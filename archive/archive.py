import collections, itertools

from jinja2 import Environment, PackageLoader

from workflow import DDLWorkflow, DMLWorkflow, Utilities

class Archive(DDLWorkflow, DMLWorkflow, Utilities):
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
    self.queries = collections.OrderedDict()

  def optimize(self, views_only = False):
    self.stats = {
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
      r._stats()

    self.stats['archive']['databases'] = len(self.stats['databases']['unique_databases'])
    self.stats['archive']['queries'] = len(self.stats['queries']['unique_queries'])
    self.stats['archive'].pop('current_depth', None)
    return self.stats

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

  def show(self):
    # Used only to set view_or_table
    self.graph(views_only = False)
    context = {
      'tables': [],
      'views': [],
      'external_tables': [],
      'statements': [],
    }

    for query in self.queries.values():
      query._show(context)

    return '''
Archive: {package} ({tables} tables, {views} views, {external_tables} external tables, and {statements} statements)

Tables:
{table_list}

Views:
{view_list}

External tables:
{external_table_list}

Statements:
{statement_list}
'''.format(
      package = self.package,
      tables = len(context['tables']),
      views = len(context['views']),
      external_tables = len(context['external_tables']),
      statements = len(context['statements']),
      table_list = str.join('\n', sorted(context['tables'])),
      view_list = str.join('\n', sorted(context['views'])),
      external_table_list = str.join('\n', sorted(context['external_tables'])),
      statement_list = str.join('\n', sorted(context['statements']))
    ).strip()

  def graph(self, views_only = False):
    context = {
      'offset': 0,
      'references': {},
    }

    graph_str = 'Archive: %s' % self.package
    for r in self.queries.values():
      graph_str += '\n%s' % r._graph(context, views_only)

    return graph_str

  def drop_all(self):
    query = self.drop_all_hql()
    if self._warn(query):
      return self.hive.run_sync(query)
    return 'Aborting'

  def drop_tables(self):
    query = self.drop_tables_hql()
    if self._warn(query):
      return self.hive.run_sync(query)
    return 'Aborting'

  def drop_tables_hql(self):
    # Used only to set view_or_table
    self.graph(views_only = False)

    # Look for tables
    tables = [t for t in self.queries.values() if hasattr(t, 'view_or_table') and t.view_or_table == 'TABLE']

    # Drop them
    return str.join('\n', ['DROP TABLE IF EXISTS %s;' % t.qualified_name() for t in tables])

  def develop(self):
    queries = self.develop_hql()
    if self._warn_all(queries):
      return self.hive.run_all_sync(queries)
    return ['Aborting']

  def develop_hql(self):
    return self._create_all_hql(views_only = True)

  def build(self):
    queries = self.build_hql()
    if self._warn_all(queries):
      return self.hive.run_all_sync(queries)
    return ['Aborting']

  def build_hql(self):
    return self._create_all_hql(views_only = False)

  def _create_all_hql(self, views_only = False):
    # Used only to set view_or_table
    self.graph(views_only = views_only)
    created = []
    return list(itertools.chain(*[query._create_sub_hql(created) for query in self.queries.values()]))

  def run(self):
    queries = self.run_hql()
    if self._warn_all(queries):
      hive_job = self.hive.run_all_sync(queries)
      return hive_job
    return ['Aborting']

  def run_hql(self):
    return list(itertools.chain(*[query.run_hql() for query in self.queries.values()]))
