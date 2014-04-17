import argparse
import importlib

class Command(object):
    def __init__(self):
        self.parser.add_argument(
            '-a', '--archive',
            required=True,
            help='name of your archive module, in the form "my.package.module"'
        )
        self.parser.add_argument(
            '-q', '--query',
            default=None,
            help='name of target query (optional)'
        )
        self.parser.set_defaults(func=self.run)

    def run(self, args):
        archive_module = importlib.import_module(args.archive)
        archive = getattr(archive_module, 'archive')

        # Propagate args everywhere
        # TODO: clean this up
        archive.args = args
        archive.hive.args = args

        # Make decision on which ViewUntilTables to materialize
        archive.optimize()

        if args.query:
            try:
                query = archive.lookup(args.query)
                query.args = args
            except KeyError:
                raise ValueError('Unrecognized query %s' % args.query)

            self.handle_query(archive, query, args)
        else:
            self.handle_archive(archive, args)

    def handle_query(self, archive, query, args):
        raise NotImplementedError('Implemented in subclasses')

    def handle_archive(self, archive, args):
        raise NotImplementedError('Implemented in subclasses')

class ArchiveCommand(Command):
    pass

class HiveCommand(Command):
    def __init__(self):
        super(HiveCommand, self).__init__()
        self.parser.add_argument(
            '-l', '--label',
            dest='label',
            default='default',
            help='label of the cluster to run on'
        )
        self.parser.add_argument(
            '-n', '--no-warn',
            dest='no_warn',
            action='store_true',
            help='do not warn on create/drop/insert queries'
        )
        self.parser.add_argument(
            '-d', '--dry',
            dest='dry',
            action='store_true',
            help='print HQL rather than executing query'
        )

class ShowCommand(ArchiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('show')
        super(ShowCommand, self).__init__()

    def handle_query(self, archive, query, args):
        raise NotImplementedError('show is not valid for queries')

    def handle_archive(self, archive, args):
        print archive.show()

class GraphCommand(ArchiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('graph')
        super(GraphCommand, self).__init__()

    def handle_query(self, archive, query, args):
        print query.graph()

    def handle_archive(self, archive, args):
        print archive.graph()

class StatsCommand(ArchiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('stats')
        super(StatsCommand, self).__init__()

    def handle_query(self, archive, query, args):
        raise NotImplementedError('stats is not valid for queries')

    def handle_archive(self, archive, args):
        import pprint
        pprint.pprint(archive.stats)

class DropAllCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('drop_all')
        super(DropAllCommand, self).__init__()

    def handle_query(self, archive, query, args):
        raise NotImplementedError('drop_all is not valid for queries')

    def handle_archive(self, archive, args):
        if args.dry:
            print archive.drop_all_hql()
        else:
            archive.drop_all()

class DropTablesCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('drop_tables')
        super(DropTablesCommand, self).__init__()

    def handle_query(self, archive, query, args):
        raise NotImplementedError('drop_tables is not valid for queries')

    def handle_archive(self, archive, args):
        if args.dry:
            print archive.drop_tables_hql()
        else:
            archive.drop_tables()

class DropCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('drop')
        super(DropCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            print query.drop_hql()
        else:
            query.drop()

    def handle_archive(self, archive, args):
        raise NotImplementedError('drop is not valid for archives')

class CreateCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('create')
        super(CreateCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            print query.create_hql()
        else:
            query.create()

    def handle_archive(self, archive, args):
        raise NotImplementedError('create is not valid for archives')

class CreateTablesCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('create_tables')
        super(CreateTablesCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            for result in query.create_tables_hql():
                print result
        else:
            for result in query.create_tables():
                print result

    def handle_archive(self, archive, args):
        raise NotImplementedError('create_tables is not valid for archives')

class RecoverPartitionsCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('recover_partitions')
        super(RecoverPartitionsCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            print query.recover_partitions_hql()
        else:
            query.recover_partitions()

    def handle_archive(self, archive, args):
        raise NotImplementedError('recover_partitions is not valid for archives')

class DevelopCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('develop')
        super(DevelopCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            for result in query.develop_hql():
                print result
        else:
            for result in query.develop():
                print result

    def handle_archive(self, archive, args):
        if args.dry:
            for result in archive.develop_hql():
                print result
        else:
            for result in archive.develop():
                print result

class BuildCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('build')
        super(BuildCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            for result in query.build_hql():
                print result
        else:
            for result in query.build():
                print result

    def handle_archive(self, archive, args):
        if args.dry:
            for result in archive.build_hql():
                print result
        else:
            for result in archive.build():
                print result

class RunCommand(HiveCommand):
    def __init__(self, subparsers):
        self.parser = subparsers.add_parser('run')
        super(RunCommand, self).__init__()

    def handle_query(self, archive, query, args):
        if args.dry:
            for result in query.run_hql():
                print result
        else:
            for result in query.run():
                print result

    def handle_archive(self, archive, args):
        if args.dry:
            for result in archive.run_hql():
                print result
        else:
            for result in archive.run():
                print result

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

ShowCommand(subparsers)
GraphCommand(subparsers)
StatsCommand(subparsers)

DropAllCommand(subparsers)
DropTablesCommand(subparsers)
DropCommand(subparsers)

CreateCommand(subparsers)
CreateTablesCommand(subparsers)
RecoverPartitionsCommand(subparsers)
DevelopCommand(subparsers)
BuildCommand(subparsers)
RunCommand(subparsers)
