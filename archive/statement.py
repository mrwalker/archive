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

  def _create_hql(self, created):
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

  def _create_sub_hql(self, created):
    # Archive may have previously created external table
    if self.external_table in created:
      external_table_create_hql = ''
    else:
      external_table_create_hql = self.external_table._create_hql(created)

    inputs_create_hql = str.join('\n', [i._create_sub_hql(created) for i in self.inputs]).strip()
    all_create_hql = """
{inputs_create_hql}

{external_table_create_hql}

-- Archive-generated HQL for insert overwrite into: {qualified_name}
{create_hql}
""".format(
      inputs_create_hql = inputs_create_hql,
      external_table_create_hql = external_table_create_hql,
      qualified_name = self.external_table.qualified_name(),
      create_hql = self._create_hql(created),
    ).strip()
    return all_create_hql

class Select(Statement):
  def _create_hql(self, created):
    return '''
{super_hql}
{hql}
;
'''.format(
      super_hql = Statement._create_hql(self, created),
      hql = self.hql(),
    ).strip()

  def _create_sub_hql(self, created):
    inputs_create_hql = str.join('\n', [i._create_sub_hql(created) for i in self.inputs]).strip()
    all_create_hql = """
{inputs_create_hql}

-- Archive-generated HQL for select: {name}
{create_hql}
""".format(
      inputs_create_hql = inputs_create_hql,
      name = self.name,
      create_hql = self._create_hql(created),
    ).strip()
    return all_create_hql
