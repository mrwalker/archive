from workflow import Utilities


class Query(Utilities):
    '''
    Archive's notion of a Hive query, Queries form the nodes of the DAG managed
    by Archive.
    '''
    def __init__(self, name, *inputs, **kwargs):
        self.name = name
        self.template = '%s.sql' % self.name
        self.inputs = inputs
        self.settings = kwargs.get('settings', {})
        self.resources = kwargs.get('resources', [])
        self.functions = kwargs.get('functions', [])

    def graph(self, **kwargs):
        return self._graph({
            'offset': 0,
            'references': {},
        }, **kwargs)

    def hql(self):
        template = self.archive.env.get_template(self.template)
        inputs = dict([(i.name, i.qualified_name()) for i in self.inputs])
        return template.render(inputs=inputs)

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
