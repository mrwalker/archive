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
    raise RuntimeError('Implemented in subclasses')

  def drop_all_hql(self):
    unique_databases = self.stats()['databases']['unique_databases']
    return str.join('\n', ['DROP DATABASE IF EXISTS %s CASCADE;' % d for d in unique_databases])

  def develop(self):
    '''
    Like build, but creates only views, not tables.  This allows you to develop
    new queries and allow Hive to check their syntax and schema prior to
    actually building the Hive.

    All relations except the immediate target are undisturbed by this method,
    allowing you to add new ones to an existing Hive.  However, you may want to
    drop everything and use build to optimize some views into tables prior to
    scheduling refreshes and runs.
    '''
    raise RuntimeError('Implemented in subclasses')

  def develop_hql(self):
    raise RuntimeError('Implemented in subclasses')

  def build(self):
    '''
    Builds a new Hive.  External tables are created and their partitions
    recovered.  Relations are created and some are optimized as tables.  If
    tables or views already exist, this will fail.
    '''
    raise RuntimeError('Implemented in subclasses')

  def build_hql(self):
    raise RuntimeError('Implemented in subclasses')

  def refresh(self):
    '''
    Refreshes an existing Hive by dropping existing tables, recovering
    partitions in external tables, and then re-creating tables.  Any new
    relations will be created.
    
    Views are not recreated, so schema changes are not supported unless you
    manually drop modified views.
    '''
    raise RuntimeError('Implemented in subclasses')

  def refresh_hql(self):
    raise RuntimeError('Implemented in subclasses')

class DMLWorkflow:
  '''
  DML workflow methods (currently only select and insert overwrite)
  '''

  def run(self):
    '''
    Executes the given query synchronously and returns its result or failure.
    This can be used in conjunction with refresh to build scheduled data
    pipelines.
    '''
    raise RuntimeError('Implemented in subclasses')

  def run_hql(self):
    raise RuntimeError('Implemented in subclasses')

class Utilities:
  '''
  Utility workflow methods
  '''

  def graph(self):
    raise RuntimeError('Implemented in subclasses')

  def stats(self):
    raise RuntimeError('Implemented in subclasses')

  def _warn(self, hql):
    '''
    If warnings are enabled, generates an stdout warning about database
    modifications contained within an HQL query. Returns a boolean
    representing whether the warning was acknowledged positively.
    '''
    if self.args and 'warn' in self.args and self.args.warn:
      if -1 != hql.find('DROP ') or \
         -1 != hql.find('INSERT ') or \
         -1 != hql.find('CREATE '):
         sys.stdout.write('This command will modify the database, are you sure you want to continue [y/n]? ')
         choice = raw_input().lower()
         return choice == 'y'
    
    return True
