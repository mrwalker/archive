from workflow import Workflow

class Query(Workflow):
  '''
  Archive's notion of a Hive query, Queries form the nodes of the DAG managed by Archive.
  '''

  def __init__(self, name, *inputs, **kwargs):
    self.name = name
    self.template = '%s.hql' % self.name
    self.inputs = inputs
    self.settings = kwargs.get('settings', {})
    self.resources = kwargs.get('resources', {})

  def graph(self, views_only = False):
    return self._graph({
      'offset': 0,
      'references': {},
    }, views_only)

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

  def develop(self):
    query = self.develop_hql()
    hive_job = self.archive.hive.run_sync(query)
    return hive_job

  def develop_hql(self):
    return self._create_all_hql(views_only = True)

  def build(self):
    query = self.build_hql()
    hive_job = self.archive.hive.run_async(query)
    return hive_job

  def build_hql(self):
    return self._create_all_hql(views_only = False)

  def _create_all_hql(self, views_only = False):
    # Used only to set view_or_table
    self.archive.graph(views_only = views_only)
    return self._create_sub_hql([])

  def create_hql(self):
    # Used only to set view_or_table
    self.archive.graph(views_only = True)
    return self._create_hql([])

  def _create_hql(self, created):
    settings_hql = ''
    for key, value in self.settings.iteritems():
      settings_hql += 'SET %s=%s;\n' % (key, value)

    resources_hql = ''
    for key, value in self.resources.iteritems():
      resources_hql += 'ADD %s %s;\n' % (key, value)

    return '%s\n%s' % (settings_hql.strip(), resources_hql.strip())
