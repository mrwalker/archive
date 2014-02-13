class Workflow:
  '''
  Workflow methods apply both to Archives and Queries.  They encapsulate the
  process of developing, building, and then refreshing your Hive.  Then intent
  is to allow a Hive to serve as the backend for an automated process for
  running data pipelines.
  '''

  def drop_all(self):
    '''
    Drops all databases of all members or dependencies.  Be careful with this;
    it's intended to give you a blank slate for development.
    '''
    query = self.drop_all_hql()
    hive_job = self.hive.run_sync(query)
    return hive_job

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
    pass

  def refresh_hql(self):
    pass

  def run(self):
    '''
    Executes the given query synchronously and returns its result or failure.
    This can be used in conjunction with refresh to build scheduled data
    pipelines.
    '''
    pass

  def run_hql(self):
    pass
