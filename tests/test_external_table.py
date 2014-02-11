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
      partitioned = True
    ))
    assert_equal('atomic.events\n', events.graph())

  def test_stats(self):
    events = self.archive.add(ExternalTable(
      'atomic',
      'events',
      partitioned = True
    ))
    stats = events.stats()
    assert_equal(1, stats['archive']['databases'])
    assert_equal(1, stats['archive']['depth'])
    assert_equal(1, stats['archive']['queries'])

    assert_equal(1, stats['databases']['references']['atomic'])
    assert_equal(set(['atomic']), stats['databases']['unique_databases'])

    assert_equal(1, stats['queries']['references']['events'])
    assert_equal(set(['events']), stats['queries']['unique_queries'])

  def test_partitioning(self):
    pattern = re.compile('.*RECOVER PARTITIONS', re.DOTALL)

    events = self.archive.add(ExternalTable(
      'atomic',
      'events',
      partitioned = True
    ))
    assert_true(pattern.match(events.create_hql([])))

    # Reset Archive
    self.setup()
    events = self.archive.add(ExternalTable(
      'atomic',
      'events'
    ))
    assert_is_none(pattern.match(events.create_hql([])))
