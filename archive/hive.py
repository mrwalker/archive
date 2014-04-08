'''
Archive's API to Hive
'''

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

from qds_sdk.qubole import Qubole as QDS
from qds_sdk.commands import *

class Backend:
  ABORT_MSG = 'Aborting'

  def run_all_sync(self, queries):
    if self._warn_all(queries):
      # Shut off individual warnings
      if 'no_warn' in self.args:
        old_warn = self.args.no_warn
        self.args.no_warn = True

      logger.info("Running %s queries" % len(queries))
      results = [self.run_sync(query) for query in queries]

      # Restore warnings
      if old_warn:
        self.args.no_warn = old_warn

      return results
    else:
      return [self.ABORT_MSG]

  def run_all_async(self, queries):
    if self._warn_all(queries):
      # Shut off individual warnings
      if 'no_warn' in self.args:
        old_warn = self.args.no_warn
        self.args.no_warn = True

      logger.info("Running %s queries" % len(queries))
      results = [self.run_async(query) for query in queries]

      # Restore warnings
      if old_warn:
        self.args.no_warn = old_warn

      return results
    else:
      return [self.ABORT_MSG]

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
         any([-1 != hql.find('CREATE ') for hql in hqls]) or \
         any([-1 != hql.find('ALTER ') for hql in hqls]):
         sys.stdout.write('This command will modify the database, are you sure you want to continue [y/n]? ')
         choice = raw_input().lower()
         return choice == 'y'

    return True

class Hive(Backend):
  def set_token(self, api_token):
    pass

  def run_sync(self, query, log_limit = 100):
    if self._warn(query):
      logger.info("Running query on dummy backend: '%s...'" % query[0:log_limit])
      return None
    else:
      return self.ABORT_MSG

  def run_async(self, query, log_limit = 100):
    if self._warn(query):
      logger.info("Running query on dummy backend: '%s...'" % query[0:log_limit])
      return None
    else:
      return self.ABORT_MSG

class Qubole(Hive):
  def set_token(self, api_token):
    QDS.configure(api_token = api_token)

  def run_sync(self, query, log_limit = 100):
    if self._warn(query):
      logger.info("Running query on Qubole backend: '%s...'" % query[0:log_limit])
      hive_command = HiveCommand.run(query = query)
      logger.info('Ran job: %s, Status: %s' % (hive_command.id, hive_command.status))

      # Notify caller if the command wasn't successful
      if not HiveCommand.is_success(hive_command.status):
        logger.error(hive_command.get_log())
        raise RuntimeError("Job %s failed or was cancelled, Status: %s\nCommand: '%s'" % (hive_command.id, hive_command.status, hive_command))

      return hive_command
    else:
      return self.ABORT_MSG

  def run_async(self, query, log_limit = 100):
    if self._warn(query):
      logger.info("Running query on Qubole backend: '%s...'" % query[0:log_limit])
      hive_command = HiveCommand.create(query = query)
      logger.info('Started job: %s, Status: %s' % (hive_command.id, hive_command.status))
      return hive_command
    else:
      return self.ABORT_MSG

if __name__ == '__main__':
  hive = Qubole()
  hive.set_token(os.environ['QUBOLE_TOKEN'])
  hive_job = hive.run_sync('SHOW TABLES;')
  hive_job.get_results()
