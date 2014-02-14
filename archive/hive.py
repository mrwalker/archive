'''
Archive's API to Hive
'''

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

from qds_sdk.qubole import Qubole as QDS
from qds_sdk.commands import *

class Hive:
  def set_token(self, api_token):
    pass

  def run_sync(self, query, log_limit = 100):
    logger.info("Running query on dummy backend: '%s...'" % query[0:log_limit])
    return None

  def run_async(self, query, log_limit = 100):
    logger.info("Running query on dummy backend: '%s...'" % query[0:log_limit])
    return None

class Qubole(Hive):
  def set_token(self, api_token):
    QDS.configure(api_token = api_token)

  def run_sync(self, query, log_limit = 100):
    logger.info("Running query on Qubole backend: '%s...'" % query[0:log_limit])
    hive_command = HiveCommand.run(query = query)
    logger.info('Ran job: %s, Status: %s' % (hive_command.id, hive_command.status))
    return hive_command

  def run_async(self, query, log_limit = 100):
    logger.info("Running query on Qubole backend: '%s...'" % query[0:log_limit])
    hive_command = HiveCommand.create(query = query)
    logger.info('Started job: %s, Status: %s' % (hive_command.id, hive_command.status))
    return hive_command

if __name__ == '__main__':
  hive = Qubole()
  hive.set_token(os.environ['QUBOLE_TOKEN'])
  hive_job = hive.run_sync('SHOW TABLES;')
  hive_job.get_results()
