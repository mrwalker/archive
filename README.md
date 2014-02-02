# Archive

Archive lets you manage your [Hive][hive] using source control.  You write
tables and views in a natural way as [HiveQL][hiveql] SELECT statements, then
scaffold the HiveQL with a Python dependency hierarchy to enable Archive to
facilitate operating on your Hive.

This approach arose out of frustration with developing an elaborate warehouse
by cutting and pasting database definitions from a git repository into Qubole's
QPal.  The abstractions likely leak all over the place and it may be awkward to
those that come from a database administration background, but it serves the
practical purpose of letting developers build and manage a Hive without having
to do something as sophisticated as build a [SQLAlchemy][sql-alchemy] backend
for HiveQL.

# Project Status

Archive is under active development at [Radico][radico-github], but is not
ready for drop-in use by others.  At this time, it should be thought of more as
a thought experiment than a production-hardened tool.

[hive]: http://hive.apache.org/
[hiveql]: https://cwiki.apache.org/confluence/display/Hive/LanguageManual
[sql-alchemy]: http://www.sqlalchemy.org/
[radico-github]: https://github.com/Radico
