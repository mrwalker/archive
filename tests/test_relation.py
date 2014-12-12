from __future__ import absolute_import

from nose.tools import *
from jinja2 import TemplateNotFound

from archive.relation import Table, View
import tests.archive

class TestRelation:
    def setup(self):
        self.archive = tests.archive.archive

    @raises(TemplateNotFound)
    def test_missing_hql(self):
        misnamed = self.archive.add(Table(
            'events',
            'misnamed'
        ))
        misnamed.hql()

class TestView:
    def setup(self):
        self.archive = tests.archive.archive
        self.view = self.archive.lookup('result_views')

    def test_graph(self):
        assert_true(self.view.graph().startswith('View(events.result_views)'))
        assert_equal(2, len(self.view.graph().split('\n')))

    def test_qualified_name(self):
        assert_equal('result_views', self.view.name)
        assert_equal('events.result_views', self.view.qualified_name())

    def test_drop_hql(self):
        assert_true('DROP VIEW' in self.view.drop_hql())

    def test_create_hql(self):
        assert_true('CREATE VIEW' in self.view.create_hql())

    def test_build_hql(self):
        build_queries = self.view.build_hql()
        assert_equal(2, len(build_queries))
        assert_equal(1, len([q for q in build_queries if 'CREATE EXTERNAL TABLE' in q]))
        assert_equal(0, len([q for q in build_queries if 'CREATE TABLE' in q]))
        assert_equal(1, len([q for q in build_queries if 'CREATE VIEW' in q]))
        assert_true('CREATE EXTERNAL TABLE' in build_queries[0])
        assert_true('CREATE VIEW' in build_queries[-1])

class TestTable:
    def setup(self):
        self.archive = tests.archive.archive
        self.table = self.archive.lookup('stage_dynamo_result_stats')

    def test_graph(self):
        assert_true(self.table.graph().startswith('Table(dynamo.stage_dynamo_result_stats)'))
        assert_equal(6, len(self.table.graph().split('\n')))

    def test_qualified_name(self):
        assert_equal('stage_dynamo_result_stats', self.table.name)
        assert_equal('dynamo.stage_dynamo_result_stats', self.table.qualified_name())

    def test_drop_hql(self):
        assert_true('DROP TABLE' in self.table.drop_hql())

    def test_create_hql(self):
        assert_true('CREATE TABLE' in self.table.create_hql())

    def test_build_hql(self):
        build_queries = self.table.build_hql()
        assert_equal(5, len(build_queries))
        assert_equal(1, len([q for q in build_queries if 'CREATE EXTERNAL TABLE' in q]))
        assert_equal(1, len([q for q in build_queries if 'CREATE TABLE' in q]))
        assert_equal(3, len([q for q in build_queries if 'CREATE VIEW' in q]))
        assert_true('CREATE EXTERNAL TABLE' in build_queries[0])
        assert_true('CREATE TABLE' in build_queries[-1])
