from __future__ import absolute_import

from nose.tools import *

from archive.hive import Hive
from archive.archive import Archive
from archive.relation import ExternalTable, ViewUntilTable

class TestArchive:
  def setup(self):
    self.archive = Archive('tests', Hive())
    self.events = ExternalTable(
      'atomic',
      'events',
      partitioned = True
    )

  def test_lookup(self):
    self.archive.add(self.events)
    events = self.archive.lookup('events')

    assert_is_not_none(events)
    assert_equal('events', events.name)
    assert_equal(self.events, events)

  @raises(KeyError)
  def test_missing_lookup(self):
    self.archive.lookup('doesnotexist')

  @raises(RuntimeError)
  def test_unique_names(self):
    self.archive.add(self.events)
    self.archive.add(self.events)

  @raises(RuntimeError)
  def test_missing_input(self):
    self.archive.add(self.events)
    doesnotexist = ViewUntilTable(
      'atomic',
      'doesnotexist',
      self.events
    )
    self.archive.add(ViewUntilTable(
      'atomic',
      'missing_input',
      doesnotexist
    ))

  def test_graph(self):
    self.archive.add(self.events)
    assert_equal('Archive: tests\nExternalTable(atomic.events)', self.archive.graph())

  def test_stats(self):
    self.archive.add(self.events)
    stats = self.archive.optimize()
    assert_equal(1, stats['archive']['databases'])
    assert_equal(1, stats['archive']['depth'])
    assert_equal(1, stats['archive']['queries'])

    assert_equal(1, stats['databases']['references']['atomic'])
    assert_equal(set(['atomic']), stats['databases']['unique_databases'])

    assert_equal(1, stats['queries']['references']['events'])
    assert_equal(set(['events']), stats['queries']['unique_queries'])
