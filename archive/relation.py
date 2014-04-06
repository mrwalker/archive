import itertools

from query import Query
from workflow import DDLWorkflow

class Relation(Query, DDLWorkflow):
  def __init__(self, database, name, *inputs, **kwargs):
    self.database = database
    Query.__init__(self, name, *inputs, **kwargs)

  def qualified_name(self):
    return '%s.%s' % (self.database, self.name)

  def _graph(self, context, views_only):
    if not context['references'].has_key(self.name):
      context['references'][self.name] = 0
    context['references'][self.name] += 1

    if context['references'][self.name] > 1:
      return '%s(%s)' % ('\t' * context['offset'], self.qualified_name())
    else:
      context['offset'] += 1
      input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
      context['offset'] -= 1

      graph_str = '%s%s\n%s' % ('\t' * context['offset'], self.qualified_name(), input_graph)
      return graph_str.rstrip()

  def _stats(self, views_only):
    stats = self.archive.stats

    stats['archive']['current_depth'] += 1
    stats['archive']['depth'] = max(
      stats['archive']['depth'],
      stats['archive']['current_depth']
    )

    stats['databases']['unique_databases'].add(self.database)
    if not stats['databases']['references'].has_key(self.database):
      stats['databases']['references'][self.database] = 0
    stats['databases']['references'][self.database] += 1

    stats['queries']['unique_queries'].add(self.name)
    if not stats['queries']['references'].has_key(self.name):
      stats['queries']['references'][self.name] = 0
    stats['queries']['references'][self.name] += 1

    if stats['queries']['references'][self.name] <= 1:
      for i in self.inputs:
        i._stats(views_only)

    stats['archive']['current_depth'] -= 1
    return stats

  def drop_all(self):
    query = self.drop_all_hql()
    if self._warn(query):
      return self.archive.hive.run_sync(query)
    return 'Aborting'

  def drop(self):
    query = self.drop_hql()
    if self._warn(query):
      return self.archive.hive.run_sync(query)
    return 'Aborting'

  def drop_hql(self):
    return 'DROP TABLE IF EXISTS %s;' % self.qualified_name()

  def develop(self):
    queries = self.develop_hql()
    if self._warn_all(queries):
      return self.archive.hive.run_all_sync(queries)
    return ['Aborting']

  def develop_hql(self):
    return self._create_all_hql(views_only = True)

  def build(self):
    queries = self.build_hql()
    if self._warn_all(queries):
      return self.archive.hive.run_all_sync(queries)
    return ['Aborting']

  def build_hql(self):
    return self._create_all_hql(views_only = False)

  def _create_database_hql(self, created):
    created_databases = set([c.database for c in created])
    if self.database in created_databases:
      return ''
    else:
      return 'CREATE DATABASE IF NOT EXISTS %s;' % self.database

  def create(self):
    query = self.create_hql()
    if self._warn(query):
      return self.archive.hive.run_sync(query)
    return 'Aborting'

  def create_hql(self):
    return self._create_hql([])

  def _create_hql(self, created):
    return '''
{command_hql}
{create_database_hql}
'''.format(
      command_hql = Query._command_hql(self),
      create_database_hql = self._create_database_hql(created)
    ).strip()

  def _create_all_hql(self, views_only = False):
    # Used only to set view_or_table
    self.archive.optimize(views_only = views_only)
    return self._create_sub_hql([])

  def _create_sub_hql(self, created):
    if self in created:
      return []
    else:
      inputs_create_hql = list(itertools.chain(*[i._create_sub_hql(created) for i in self.inputs]))
      create_hql = self._create_hql(created)
      inputs_create_hql.append(create_hql)
      created.append(self)
      return inputs_create_hql

class ExternalTable(Relation):
  def __init__(self, database, name, *inputs, **kwargs):
    Relation.__init__(self, database, name, *inputs, **kwargs)
    self.partitioned = kwargs.get('partitioned', False)

  def _show(self, context):
    context['external_tables'].append(self.qualified_name())

  def recover_partitions(self):
    query = self.recover_partitions_hql()
    if self._warn(query):
      return self.archive.hive.run_sync(query)
    return 'Aborting'

  def recover_partitions_hql(self):
    if self.partitioned:
      return 'ALTER TABLE `{database}.{name}` RECOVER PARTITIONS;'.format(
        database = self.database,
        name = self.name,
      )
    else:
      return ''

  def _create_hql(self, created):
    return '''
{super_hql}
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{name}
{hql}
;

{recover_partitions_hql}
'''.format(
      super_hql = Relation._create_hql(self, created),
      database = self.database,
      name = self.name,
      hql = self.hql(),
      recover_partitions_hql = self.recover_partitions_hql()
    ).strip()

class ViewUntilTable(Relation):
  def __init__(self, database, name, *inputs, **kwargs):
    Relation.__init__(self, database, name, *inputs, **kwargs)
    self.view_or_table = None
    self.table_threshold = 3

  def _show(self, context):
    if self.view_or_table == 'TABLE':
      context['tables'].append(self.qualified_name())
    else:
      context['views'].append(self.qualified_name())

  def drop_hql(self):
    return 'DROP %s IF EXISTS %s;' % (
      self.view_or_table,
      self.qualified_name()
    )

  def _create_hql(self, created):
    if not self.view_or_table:
      raise RuntimeError('Create type must be determined before calling ViewUntilTable#_create_hql')

    return '''
{super_hql}
CREATE {view_or_table} IF NOT EXISTS {database}.{name} AS
{hql}
;
'''.format(
      super_hql = Relation._create_hql(self, created),
      view_or_table = self.view_or_table,
      database = self.database,
      name = self.name,
      hql = self.hql(),
    ).strip()

  def _stats(self, views_only):
    stats = Relation._stats(self, views_only)

    if not views_only and stats['queries']['references'][self.name] >= self.table_threshold:
      self.view_or_table = 'TABLE'
    else:
      self.view_or_table = 'VIEW'

    return stats

class Table(ViewUntilTable):
  def _stats(self, views_only):
    self.view_or_table = 'TABLE' if not views_only else 'VIEW'
    return Relation._stats(self, views_only)

class View(ViewUntilTable):
  def _stats(self, views_only):
    self.view_or_table = 'VIEW'
    return Relation._stats(self, views_only)
