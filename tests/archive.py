from __future__ import absolute_import

import os

from archive.archive import Archive
from archive.hive import Hive, Qubole

import tests.databases as databases

ARCHIVE_ENV = os.environ['ARCHIVE_ENV']
if ARCHIVE_ENV == 'prod':
  hive = Qubole()
  hive.set_token(os.environ['QUBOLE_TOKEN'])
else:
  hive = Hive()
archive = Archive('tests', hive)

atomic_database = 'atomic'
inputs_database = 'inputs'
events_database = 'events'
dynamo_database = 'dynamo'

# Order is important
databases.atomic(archive, atomic_database)
databases.inputs(archive, inputs_database)
databases.events(archive, events_database)
databases.dynamo(archive, dynamo_database)
