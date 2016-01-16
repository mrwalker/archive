'''
Workflow methods apply both to Archives and Queries.  They encapsulate the
process of building and then refreshing your Hive.  The intent is to allow a
Hive to serve as the backend for an automated process for running data
pipelines.
'''


class DDLWorkflow(object):
    '''
    DDL workflow methods (create and delete)
    '''
    def drop_all(self):
        '''
        Drops all databases of all members or dependencies.  Be careful with
        this; it's intended to give you a blank slate for development.
        '''
        raise NotImplementedError('Implemented in subclasses')

    def drop_all_hql(self):
        unique_databases = self.stats['databases']['unique_databases']
        return str.join('\n', [
            'DROP DATABASE IF EXISTS %s CASCADE;' % d
            for d in unique_databases
        ])

    def drop_tables(self):
        '''
        Drops only tables, which allows them to be refreshed using build.
        '''
        raise NotImplementedError('Implemented in subclasses')

    def drop_tables_hql(self):
        raise NotImplementedError('Implemented in subclasses')

    def recover_all(self):
        '''
        Recovers partitions for all external tables.  This can be used if your
        refresh scheme is to drop and re-create only tables.
        '''
        raise NotImplementedError('Implemented in subclasses')

    def recover_all_hql(self):
        raise NotImplementedError('Implemented in subclasses')

    def build(self):
        '''
        Builds a new Hive.  External tables are created and their partitions
        recovered.  Relations are created and tables populated.
        '''
        raise NotImplementedError('Implemented in subclasses')

    def build_hql(self):
        raise NotImplementedError('Implemented in subclasses')


class DMLWorkflow(object):
    '''
    DML workflow methods (currently only select and insert overwrite)
    '''
    def run(self):
        '''
        Executes the given query synchronously and returns its result or
        failure.  This can be used in conjunction with drop_tables/build to
        build scheduled data pipelines.
        '''
        raise NotImplementedError('Implemented in subclasses')

    def run_hql(self):
        raise NotImplementedError('Implemented in subclasses')


class Utilities(object):
    '''
    Utility workflow methods
    '''
    def graph(self):
        raise NotImplementedError('Implemented in subclasses')
