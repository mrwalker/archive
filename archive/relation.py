from jinja2 import Environment, PackageLoader

class Relation:
  # Set this to the package that contains your archive
  JINJA_PACKAGE = None

  def __init__(self, database, name, *inputs, **kwargs):
    self.package = self.JINJA_PACKAGE
    self.template = '%s.hql' % name

    templates = kwargs.get('templates', 'templates')
    self.env = Environment(loader = PackageLoader(
      self.package,
      templates
    ))

    self.database = database
    self.name = name
    self.inputs = inputs

  def qualified_name(self):
    return '%s.%s' % (self.database, self.name)

  def graph(self):
    return self._graph({
      'offset': 0,
      'traversed': set(),
    })

  def _graph(self, context):
    if self in context['traversed']:
      return '%s(%s)' % ('\t' * context['offset'], self.qualified_name())
    else:
      context['traversed'].add(self)
      context['offset'] += 1
      input_graph = str.join('\n', [i._graph(context) for i in self.inputs]).rstrip()
      context['offset'] -= 1

      graph_str = '%s%s\n%s' % ('\t' * context['offset'], self.qualified_name(), input_graph)
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

    stats['databases']['unique_databases'].add(self.database)
    if not stats['databases']['references'].has_key(self.database):
      stats['databases']['references'][self.database] = 0
    stats['databases']['references'][self.database] += 1

    stats['relations']['unique_relations'].add(self.name)
    if not stats['relations']['references'].has_key(self.name):
      stats['relations']['references'][self.name] = 0
    stats['relations']['references'][self.name] += 1

    for i in self.inputs:
      i._stats(stats)

    stats['archive']['current_depth'] -= 1
    return stats

  def hql(self):
    template = self.env.get_template(self.template)
    inputs = dict([(i.name, i.qualified_name()) for i in self.inputs])
    return template.render(inputs = inputs)

  def create_hql(self, created):
    created_databases = set([c.database for c in created])
    if self.database in created_databases:
      return ''
    else:
      return 'CREATE DATABASE IF NOT EXISTS %s;' % self.database

  def create_all_hql(self):
    # Used only to set view_or_table
    self.graph()
    return self._create_all_hql([])

  def _create_all_hql(self, created):
    if self in created:
      return ''
    else:
      inputs_create_hql = str.join('\n', [i._create_all_hql(created) for i in self.inputs]).strip()
      all_create_hql = """{inputs_create_hql}

-- Archive-generated HQL for: {qualified_name}
-- Created: {created_qualified_names}
{create_hql}""".format(
        inputs_create_hql = inputs_create_hql,
        qualified_name = self.qualified_name(),
        created_qualified_names = [c.qualified_name() for c in created],
        create_hql = self.create_hql(created),
      )
      created.append(self)
      return all_create_hql

  def drop_all_hql(self):
    unique_databases = self.stats()['databases']['unique_databases']
    return str.join('\n', ['DROP DATABASE IF EXISTS %s CASCADE;' % d for d in unique_databases])

class ExternalTable(Relation):
  def create_hql(self, created):
    return '''{super_hql}
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{name}
{hql}
;'''.format(
      super_hql = Relation.create_hql(self, created),
      database = self.database,
      name = self.name,
      hql = self.hql(),
    ).strip()

class ViewUntilTable(Relation):
  def __init__(self, database, name, *inputs, **kwargs):
    Relation.__init__(self, database, name, *inputs, **kwargs)
    self.view_or_table = None

  def create_hql(self, created):
    if not self.view_or_table:
      raise RuntimeError('Create type must be determined before calling ViewUntilTable#create_hql')

    return '''{super_hql}
CREATE {view_or_table} IF NOT EXISTS {database}.{name} AS
{hql}
;'''.format(
      super_hql = Relation.create_hql(self, created),
      view_or_table = self.view_or_table,
      database = self.database,
      name = self.name,
      hql = self.hql(),
    ).strip()

  def _graph(self, context):
    if self in context['traversed']:
      self.view_or_table = 'TABLE'
    else:
      self.view_or_table = 'VIEW'
    return Relation._graph(self, context)
