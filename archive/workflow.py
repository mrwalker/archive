'''
Workflow methods apply both to Archives and Queries.  They encapsulate the
process of developing, building, and then refreshing your Hive.  The intent
is to allow a Hive to serve as the backend for an automated process for
running data pipelines.
'''

import sys

class DDLWorkflow:
  '''
  DDL workflow methods (create and delete)
  '''

  def drop_all(self):
    '''
    Drops all databases of all members or dependencies.  Be careful with this;
    it's intended to give you a blank slate for development.
    '''
    raise NotImplementedError('Implemented in subclasses')

  def drop_all_hql(self):
    unique_databases = self.stats['databases']['unique_databases']
    return str.join('\n', ['DROP DATABASE IF EXISTS %s CASCADE;' % d for d in unique_databases])

  def drop_tables(self):
    '''
    Drops only tables, which allows them to be refreshed using build.
    '''
    raise NotImplementedError('Implemented in subclasses')

  def drop_tables_hql(self):
    raise NotImplementedError('Implemented in subclasses')

  def develop(self):
    '''
    Like build, but creates only views, not tables.  This allows you to develop
    new queries and allows Hive to check their syntax and schema prior to
    actually building the Hive.

    All relations except the immediate target are undisturbed by this method,
    allowing you to add new ones to an existing Hive.  However, you may want to
    drop everything and use build to optimize some views into tables prior to
    scheduling refreshes and runs.
    '''
    raise NotImplementedError('Implemented in subclasses')

  def develop_hql(self):
    raise NotImplementedError('Implemented in subclasses')

  def build(self):
    '''
    Builds a new Hive.  External tables are created and their partitions
    recovered.  Relations are created and some are optimized as tables.
    '''
    raise NotImplementedError('Implemented in subclasses')

  def build_hql(self):
    raise NotImplementedError('Implemented in subclasses')

class DMLWorkflow:
  '''
  DML workflow methods (currently only select and insert overwrite)
  '''

  def run(self):
    '''
    Executes the given query synchronously and returns its result or failure.
    This can be used in conjunction with drop_tables/build to build scheduled
    data pipelines.
    '''
    raise NotImplementedError('Implemented in subclasses')

  def run_hql(self):
    raise NotImplementedError('Implemented in subclasses')

class Utilities:
  '''
  Utility workflow methods
  '''

  def graph(self):
    raise NotImplementedError('Implemented in subclasses')

  def _warn(self, hql):
    '''
    If warnings are enabled, generates an stdout warning about database
    modifications contained within an HQL query. Returns a boolean
    representing whether the warning was acknowledged positively.
    '''
    return self._warn_all([hql])

  def _warn_all(self, hqls):
    '''
    If warnings are enabled, generates an stdout warning about database
    modifications contained within a list of HQL queries. Returns a boolean
    representing whether the warning was acknowledged positively.
    '''
    if self.args and 'no_warn' in self.args and not self.args.no_warn:
      if any([-1 != hql.find('DROP ') for hql in hqls]) or \
         any([-1 != hql.find('INSERT ') for hql in hqls]) or \
         any([-1 != hql.find('CREATE ') for hql in hqls]):
         sys.stdout.write('This command will modify the database, are you sure you want to continue [y/n]? ')
         choice = raw_input().lower()
         return choice == 'y'

    return True
