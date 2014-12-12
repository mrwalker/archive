'''
Test Archive

Until Archive actually supports explicit databses, this is how you define them.
'''
from __future__ import absolute_import

from archive.relation import ExternalTable, Table, View
from archive.statement import InsertOverwrite, Select

def atomic(archive, atomic_database):
    events = archive.add(ExternalTable(
        atomic_database,
        'events',
        partitioned=True
    ))

def inputs(archive, inputs_database):
    '''
    Database built atop atomic that serves as the actual input to downstream
    relations.
    '''
    events = archive.lookup('events')

    # Event table partitioned by event for faster individual event ETL
    partitioned_events = archive.add(ExternalTable(
        inputs_database,
        'partitioned_events',
        partitioned=True
    ))

    # Writes partitioned_events with support for fully dynamic partitions and
    # appropriate parallelism for bucketing
    insert_overwrite_partitioned_events = archive.add(InsertOverwrite(
        'insert_overwrite_partitioned_events',
        partitioned_events,
        events,
        settings = {
            'hive.exec.dynamic.partition.mode': 'nonstrict',
            'hive.enforce.bucketing': 'true',
        }
    ))
