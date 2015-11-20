# -*- coding: utf-8 -*-
import luigi
from luigi import parameter
from .postgres import Postgres
from .s3 import S3


_redshift_unload_template = '''\
UNLOAD('{statement}')
TO '{s3path}'
CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
{options};'''

_redshift_copy_template = '''\
COPY {statement}
FROM '{s3path}'
CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
{options};'''


class UnloadToS3(luigi.Task):
    s3conf = luigi.Parameter(default='s3')
    s3path = luigi.Parameter()
    rsconf = luigi.Parameter()
    select = luigi.Parameter()
    parallel_off = luigi.BoolParameter(default=False)
    options = luigi.Parameter(default='')

    def run(self):
        if self.options and 'parallel' in self.options.lower():
            raise parameter.ParameterException('options cannot contain "parallel". use --parallel-off instead.')
        options = self.options or "GZIP ADDQUOTES ESCAPE ALLOWOVERWRITE DELIMITER ','"
        if 'manifest' not in options.lower():
            options += ' MANIFEST'
        if self.parallel_off:
            options += " PARALLEL OFF"
        try:
            with open(self.select, 'r') as fp:
                select = fp.read()
        except:
            select = self.select

        context = {
            'options': options,
            'statement': select.replace("'", "''"),
            's3path': self.s3path,
        }
        context.update(S3(self.s3conf).conf)
        sql = _redshift_unload_template.format(**context)
        Postgres(self.rsconf).run_sql(sql)

    def output(self):
        return S3(self.s3conf).get_target(self.s3path + 'manifest')


class CopyFromS3(luigi.Task):
    s3conf = luigi.Parameter(default='s3')
    s3path = luigi.Parameter()
    rsconf = luigi.Parameter()
    table = luigi.Parameter()
    options = luigi.Parameter(default='')
    manifest = luigi.BoolParameter(default=False)
    gzip_off = luigi.BoolParameter(default=False)
    csv = luigi.BoolParameter(default=False)
    delimiter = luigi.Parameter(default=',')
    drop = luigi.BoolParameter(default=False)
    table_schema = luigi.Parameter(default='')
    truncate = luigi.BoolParameter(default=False)

    def get_sql(self):
        if not self.options:
            options = "TRUNCATECOLUMNS DELIMITER '%s' " % self.delimiter
            options += not self.gzip_off and 'GZIP ' or ''
            options += self.csv and 'CSV ' or 'REMOVEQUOTES ESCAPE '
            options += self.manifest and 'MANIFEST' or ''
        else:
            options = self.options

        context = {
            'options': options,
            'statement': self.table,
            'table': self.table.strip().split(' ', 1)[0],
            'table_schema': self.table_schema,
            's3path': self.s3path,
        }
        context.update(S3(self.s3conf).conf)

        # generate copy sql
        sql = ''
        if self.drop:
            sql += 'drop table if exists {table};\n'
        if self.table_schema:
            sql += 'create table if not exists {table} {table_schema};\n'
        if self.truncate:
            sql += 'truncate {table};'
        sql += _redshift_copy_template
        return sql.format(**context)

    def run(self):
        Postgres(self.rsconf).run_sql(self.get_sql())
        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)
