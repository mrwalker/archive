from query import Query
from workflow import DMLWorkflow

class Statement(Query, DMLWorkflow):
  def _graph(self, context, views_only):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
    context['offset'] -= 1

    graph_str = '%s[%s]\n%s' % ('\t' * context['offset'], self.name, input_graph)
    return graph_str.rstrip()

  def _stats(self, stats):
    stats['archive']['current_depth'] += 1
    stats['archive']['depth'] = max(
      stats['archive']['depth'],
      stats['archive']['current_depth']
    )

    for i in self.inputs:
      i._stats(stats)

    stats['archive']['current_depth'] -= 1
    return stats

  def _show(self, context):
    context['statements'].append(self.name)

  def _create_sub_hql(self, created):
    return ''

  def run(self):
    query = self.run_hql()
    if self._warn(query):
      hive_job = self.archive.hive.run_async(query)
      return hive_job
    return 'Aborting.'

  def run_hql(self):
    return self._run_hql([])

class InsertOverwrite(Statement):
  def __init__(self, name, external_table, *inputs, **kwargs):
    self.external_table = external_table
    Statement.__init__(self, name, *inputs, **kwargs)

  def _graph(self, context, views_only):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
    external_table_graph = self.external_table._graph(context, views_only)
    context['offset'] -= 1

    graph_str = '%s[%s]\n%s\n%s' % ('\t' * context['offset'], self.name, input_graph, external_table_graph)
    return graph_str

  def _run_hql(self, created):
    return '''
{super_hql}
INSERT OVERWRITE TABLE {database}.{name}
{hql}
;
'''.format(
      super_hql = Statement._create_hql(self, created),
      database = self.external_table.database,
      name = self.external_table.name,
      hql = self.hql(),
    ).strip()

class Select(Statement):
  def _run_hql(self, created):
    return '''
{super_hql}
{hql}
;
'''.format(
      super_hql = Statement._create_hql(self, created),
      hql = self.hql(),
    ).strip()
