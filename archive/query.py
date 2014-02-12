from workflow import Workflow

class Query(Workflow):
  '''
  Archive's notion of a Hive query, Queries form the nodes of the DAG managed by Archive.
  '''

  def __init__(self, name, *inputs, **kwargs):
    self.name = name
    self.template = '%s.hql' % self.name
    self.inputs = inputs

  def graph(self):
    return self._graph({
      'offset': 0,
      'references': {},
    })

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
      'queries': {
        'unique_queries': set(),
        'references': {},
      }
    })
    stats['archive']['databases'] = len(stats['databases']['unique_databases'])
    stats['archive']['queries'] = len(stats['queries']['unique_queries'])
    stats['archive'].pop('current_depth', None)
    return stats

  def hql(self):
    template = self.archive.env.get_template(self.template)
    inputs = dict([(i.name, i.qualified_name()) for i in self.inputs])
    return template.render(inputs = inputs)

  def create_all(self):
    query = self.create_all_hql()
    hive_job = self.archive.hive.run_async(query)
    return hive_job

  def create_all_hql(self):
    # Used only to set view_or_table
    self.graph()
    return self._create_all_hql([])
