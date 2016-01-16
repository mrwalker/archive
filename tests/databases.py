'''
Test Archive

Until Archive actually supports explicit databses, this is how you define them.
'''
from __future__ import absolute_import

from archive.relation import ExternalTable, Table, View
from archive.statement import InsertOverwrite


def atomic(archive, atomic_database):
    archive.add(ExternalTable(
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
    archive.add(InsertOverwrite(
        'insert_overwrite_partitioned_events',
        partitioned_events,
        events,
        settings={
            'hive.exec.dynamic.partition.mode': 'nonstrict',
            'hive.enforce.bucketing': 'true',
        }
    ))


def events(archive, events_database):
    partitioned_events = archive.lookup('partitioned_events')

    searches = archive.add(View(
        events_database,
        'searches',
        partitioned_events
    ))
    archive.add(View(
        events_database,
        'impressions',
        searches
    ))

    archive.add(View(
        events_database,
        'result_views',
        partitioned_events
    ))


def dynamo(archive, dynamo_database):
    impressions = archive.lookup('impressions')
    result_views = archive.lookup('result_views')

    stage_dynamo_result_stats = archive.add(Table(
        dynamo_database,
        'stage_dynamo_result_stats',
        impressions,
        result_views
    ))

    dynamo_result_stats = archive.add(ExternalTable(
        dynamo_database,
        'dynamo_result_stats',
        resources=[{
            'type': 'JAR',
            'path': (
                's3://paid-qubole/dynamoDB/jars/'
                'hadoop-dynamodb-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
            ),
        }]
    ))
    archive.add(InsertOverwrite(
        'insert_overwrite_dynamo_result_stats',
        dynamo_result_stats,
        stage_dynamo_result_stats,
        resources=[{
            'type': 'JAR',
            'path': (
                's3://paid-qubole/dynamoDB/jars/'
                'hadoop-dynamodb-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
            ),
        }],
        settings={
            'hive.mapred.map.tasks.speculative.execution': 'false',
            'hive.mapred.reduce.tasks.speculative.execution': 'false',
            'hive.exec.reducers.max': '1',
        }
    ))
