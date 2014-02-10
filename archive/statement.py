class InsertOverwrite:
  def __init__(self, name, external_table, *inputs):
    self.external_table = external_table
    self.name = name
    self.template = '%s.hql' % self.name
    self.inputs = inputs

  def graph(self):
    return self._graph({
      'offset': 0,
      'references': {},
    })

  def _graph(self, context):
    context['offset'] += 1
    input_graph = str.join('\n', [i._graph(context) for i in self.inputs]).rstrip()
    external_table_graph = self.external_table._graph(context)
    context['offset'] -= 1

    graph_str = '%s%s\n%s\n%s' % ('\t' * context['offset'], self.name, input_graph, external_table_graph)
    return graph_str

  def stats(self):
    stats = self._stats({
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
    })
    stats['archive']['databases'] = len(stats['databases']['unique_databases'])
    stats['archive']['relations'] = len(stats['relations']['unique_relations'])
    stats['archive'].pop('current_depth', None)
    return stats

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

  def hql(self):
    template = self.archive.env.get_template(self.template)
    inputs = dict([(i.name, i.qualified_name()) for i in self.inputs])
    return template.render(inputs = inputs)

  def create_hql(self, created):
    return '''INSERT OVERWRITE TABLE {database}.{name}
{hql}
;'''.format(
      database = self.external_table.database,
      name = self.external_table.name,
      hql = self.hql(),
    ).strip()

  def create_all(self):
    query = self.create_all_hql()
    hive_job = self.archive.hive.run_async(query)
    return hive_job

  def create_all_hql(self):
    # Used only to set view_or_table
    self.graph()
    return self._create_all_hql([])

  def _create_all_hql(self, created):
    inputs_create_hql = str.join('\n', [i._create_all_hql(created) for i in self.inputs]).strip()
    all_create_hql = """{inputs_create_hql}

{external_table_create_hql}

-- Archive-generated HQL for insert overwrite into: {qualified_name}
-- Created: {created_qualified_names}
{create_hql}""".format(
      inputs_create_hql = inputs_create_hql,
      external_table_create_hql = self.external_table.create_hql(created),
      qualified_name = self.external_table.qualified_name(),
      created_qualified_names = [c.qualified_name() for c in created],
      create_hql = self.create_hql(created),
    )
    return all_create_hql

  def drop_all(self):
    query = self.drop_all_hql()
    hive_job = self.archive.hive.run_sync(query)
    return hive_job

  def drop_all_hql(self):
    unique_databases = self.stats()['databases']['unique_databases']
    return str.join('\n', ['DROP DATABASE IF EXISTS %s CASCADE;' % d for d in unique_databases])
