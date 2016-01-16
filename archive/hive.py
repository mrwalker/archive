'''
Archive's API to Hive
'''

import logging
import time

from requests.exceptions import ConnectionError

from qds_sdk.qubole import Qubole as QDS
from qds_sdk.commands import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Backend(object):
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
                sys.stdout.write((
                    'This command will modify the database, '
                    'are you sure you want to continue [y/n]? '
                ))
                choice = raw_input().lower()
                return choice == 'y'

        return True


class Hive(Backend):
    def set_token(self, api_token):
        pass

    def run_sync(self, query, log_limit=100):
        if self._warn(query):
            logger.info(
                "Running query on dummy backend: '%s...'" % query[0:log_limit]
            )
            return None
        else:
            return self.ABORT_MSG

    def run_async(self, query, log_limit=100):
        if self._warn(query):
            logger.info(
                "Running query on dummy backend: '%s...'" % query[0:log_limit]
            )
            return None
        else:
            return self.ABORT_MSG


class Qubole(Hive):
    def set_token(self, api_token):
        QDS.configure(api_token=api_token)

    def backoff_poll_interval(self, multiple=2):
        QDS.configure(QDS.api_token, poll_interval=QDS.poll_interval * multiple)

    def poll_with_retries(self, hive_command, retries=3):
        '''
        A temporary work-around for connection failures while polling QDS.  See
        qds_sdk.commands.Command#run for the original source of this code,
        prior to retries with polling interval backoff.
        '''
        try:
            while not HiveCommand.is_done(hive_command.status):
                time.sleep(QDS.poll_interval)
                hive_command = HiveCommand.find(hive_command.id)

            logger.info('Ran job: %s, Status: %s' % (
                hive_command.id,
                hive_command.status
            ))

            # Notify caller if the command wasn't successful
            if not HiveCommand.is_success(hive_command.status):
                logger.error(hive_command.get_log())
                raise RuntimeError((
                    'Job %s failed or was cancelled, '
                    "Status: %s\nCommand: '%s'" % (
                        hive_command.id,
                        hive_command.status,
                        hive_command
                    )
                ))

            return hive_command
        except ConnectionError as error:
            if retries > 0:
                self.backoff_poll_interval()
                logger.error((
                    'Received ConnectionError: %s, '
                    'Retrying with polling interval: %s (%s remaining)' % (
                        error,
                        QDS.poll_interval,
                        retries
                    )
                ))
                return self.poll_with_retries(
                    hive_command,
                    retries=retries - 1
                )
            else:
                logger.exception('Polling retries exhausted')

    def run_sync(self, query, log_limit=100, retries=3):
        if self._warn(query):
            logger.info(
                "Running query on Qubole backend: '%s...'" % query[0:log_limit]
            )

            kwargs = {'query': query}
            if 'label' in self.args:
                kwargs['label'] = self.args.label

            hive_command = HiveCommand.create(**kwargs)
            return self.poll_with_retries(hive_command)
        else:
            return self.ABORT_MSG

    def run_async(self, query, log_limit=100):
        if self._warn(query):
            logger.info(
                "Running query on Qubole backend: '%s...'" % query[0:log_limit]
            )

            kwargs = {'query': query}
            if 'label' in self.args:
                kwargs['label'] = self.args.label

            hive_command = HiveCommand.create(**kwargs)

            logger.info('Started job: %s, Status: %s' % (
                hive_command.id,
                hive_command.status
            ))
            return hive_command
        else:
            return self.ABORT_MSG

if __name__ == '__main__':
    hive = Qubole()
    hive.set_token(os.environ['QUBOLE_TOKEN'])

    from argparse import Namespace
    args = Namespace()
    # args.label = 'prod'
    hive.args = args

    hive_job = hive.run_sync('SHOW TABLES;')
    hive_job.get_results()
