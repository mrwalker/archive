from query import Query
from workflow import DMLWorkflow

class Statement(Query, DMLWorkflow):
  def _graph(self, context, views_only):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
    context['offset'] -= 1

    graph_str = '%s[%s]\n%s' % ('\t' * context['offset'], self.name, input_graph)
    return graph_str.rstrip()

  def _stats(self, views_only):
    stats = self.archive.stats

    stats['archive']['current_depth'] += 1
    stats['archive']['depth'] = max(
      stats['archive']['depth'],
      stats['archive']['current_depth']
    )

    for i in self.inputs:
      i._stats(views_only)

    stats['archive']['current_depth'] -= 1
    return stats

  def _show(self, context):
    context['statements'].append(self.name)

  def run(self):
    return self.archive.hive.run_all_sync(self.run_hql())

class InsertOverwrite(Statement):
  def __init__(self, name, external_table, *inputs, **kwargs):
    self.external_table = external_table
    Statement.__init__(self, name, *inputs, **kwargs)

  def __str__(self):
    return 'InsertOverwrite(%s, %s)' % (self.name, self.external_table)

  def _graph(self, context, views_only):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
    external_table_graph = self.external_table._graph(context, views_only)
    context['offset'] -= 1

    graph_str = '%s[%s]\n%s\n%s' % ('\t' * context['offset'], self.name, input_graph, external_table_graph)
    return graph_str

  def _stats(self, views_only):
    Statement._stats(self, views_only)
    stats = self.external_table._stats(views_only)

  def run_hql(self):
    return ['''
{command_hql}
INSERT OVERWRITE TABLE {database}.{name}
{hql}
;
'''.format(
      command_hql = Statement._command_hql(self),
      database = self.external_table.database,
      name = self.external_table.name,
      hql = self.hql(),
    ).strip()]

class Select(Statement):
  def __str__(self):
    return 'Select(%s, %s)' % (self.name)

  def run_hql(self):
    return ['''
{command_hql}
{hql}
;
'''.format(
      command_hql = Statement._command_hql(self),
      hql = self.hql(),
    ).strip()]
