from __future__ import absolute_import

import re

from nose.tools import *
from jinja2 import TemplateNotFound

from archive.hive import Hive
from archive.archive import Archive
from archive.relation import ExternalTable

class TestExternalTable:
    def setup(self):
        self.archive = Archive('tests', Hive())

    @raises(TemplateNotFound)
    def test_missing_hql(self):
        misnamed = self.archive.add(ExternalTable(
            'atomic',
            'misnamed'
        ))
        misnamed.hql()

    def test_graph(self):
        events = self.archive.add(ExternalTable(
            'atomic',
            'events',
            partitioned=True
        ))
        assert_equal('ExternalTable(atomic.events)', events.graph())

    def test_partitioning(self):
        pattern = re.compile('.*RECOVER PARTITIONS', re.DOTALL)

        events = self.archive.add(ExternalTable(
            'atomic',
            'events',
            partitioned=True
        ))
        assert_true(pattern.match(events.create_hql()))

        # Reset Archive
        self.setup()
        events = self.archive.add(ExternalTable(
            'atomic',
            'events'
        ))
        assert_is_none(pattern.match(events.create_hql()))
