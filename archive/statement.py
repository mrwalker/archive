from query import Query

class InsertOverwrite(Query):
  def __init__(self, name, external_table, *inputs, **kwargs):
    self.external_table = external_table
    Query.__init__(self, name, *inputs, **kwargs)

  def _graph(self, context, views_only):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context, views_only) for i in self.inputs]).rstrip()
    external_table_graph = self.external_table._graph(context, views_only)
    context['offset'] -= 1

    graph_str = '%s%s\n%s\n%s' % ('\t' * context['offset'], self.name, input_graph, external_table_graph)
    return graph_str

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

  def _create_hql(self, created):
    return '''
{super_hql}
INSERT OVERWRITE TABLE {database}.{name}
{hql}
;
'''.format(
      super_hql = Query._create_hql(self, created),
      database = self.external_table.database,
      name = self.external_table.name,
      hql = self.hql(),
    ).strip()

  def _create_sub_hql(self, created):
    inputs_create_hql = str.join('\n', [i._create_sub_hql(created) for i in self.inputs]).strip()
    all_create_hql = """{inputs_create_hql}

{external_table_create_hql}

-- Archive-generated HQL for insert overwrite into: {qualified_name}
-- Created: {created_qualified_names}
{create_hql}""".format(
      inputs_create_hql = inputs_create_hql,
      external_table_create_hql = self.external_table._create_hql(created),
      qualified_name = self.external_table.qualified_name(),
      created_qualified_names = [c.qualified_name() for c in created],
      create_hql = self._create_hql(created),
    )
    return all_create_hql
