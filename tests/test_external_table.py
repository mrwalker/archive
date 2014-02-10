from nose.tools import *

from jinja2 import TemplateNotFound

from archive.hive import Hive
from archive.archive import Archive
from archive.relation import ExternalTable

class TestExternalTable:
  def setup(self):
    self.archive = Archive('tests', Hive())
    self.events = self.archive.add(ExternalTable(
      'atomic',
      'events'
    ))

  @raises(TemplateNotFound)
  def test_missing_hql(self):
    misnamed = self.archive.add(ExternalTable(
      'atomic',
      'misnamed'
    ))
    misnamed.hql()

  def test_graph(self):
    assert_equal('atomic.events\n', self.events.graph())

  def test_stats(self):
    stats = self.events.stats()
    assert_equal(1, stats['archive']['databases'])
    assert_equal(1, stats['archive']['depth'])
    assert_equal(1, stats['archive']['relations'])

    assert_equal(1, stats['databases']['references']['atomic'])
    assert_equal(set(['atomic']), stats['databases']['unique_databases'])

    assert_equal(1, stats['relations']['references']['events'])
    assert_equal(set(['events']), stats['relations']['unique_relations'])
