from workflow import Utilities

class Query(Utilities):
  '''
  Archive's notion of a Hive query, Queries form the nodes of the DAG managed by Archive.
  '''

  def __init__(self, name, *inputs, **kwargs):
    self.name = name
    self.template = '%s.hql' % self.name
    self.inputs = inputs
    self.settings = kwargs.get('settings', {})
    self.resources = kwargs.get('resources', [])
    self.functions = kwargs.get('functions', [])

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

  def _command_hql(self):
    resources_hql = ''
    for config in self.resources:
      resources_hql += 'ADD %s %s;\n' % (config['type'], config['path'])

    functions_hql = ''
    for config in self.functions:
      functions_hql += "CREATE TEMPORARY FUNCTION %s AS '%s';\n" % (
        config['function'],
        config['class']
      )

    settings_hql = ''
    for key, value in self.settings.iteritems():
      settings_hql += 'SET %s=%s;\n' % (key, value)

    return '%s\n%s\n%s' % (
      resources_hql.strip(),
      functions_hql.strip(),
      settings_hql.strip()
    )

  def run_hql(self):
    return []

  def _create_sub_hql(self, created):
    return []
