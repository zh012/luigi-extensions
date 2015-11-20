from hashlib import md5
import luigi
import luigi.format


def guess_format(f):
    if f.endswith('.gz'):
        return luigi.format.Gzip
    return luigi.format.Text


class LocalPathTask(luigi.ExternalTask):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path, format=guess_format(self.path))


def source_task(source, **options):
    from .redshift import UnloadToS3
    from .s3 import S3PathTask
    if source.startswith('redshift://'):
        source, is_script_file = source[11:], True
        task = UnloadToS3(
            s3conf=options['s3conf'],
            dbconf=options['rsconf'],
            s3path=options['s3path'],
            select_file=source)
    elif source.startswith('redshift+inline://'):
        source, is_script_file = source[18:], False
        task = UnloadToS3(
            s3conf=options['s3conf'],
            dbconf=options['rsconf'],
            s3path=options['s3path'],
            select=source)
    elif source.startswith('s3://'):
        task = S3PathTask(path=source, s3conf=options['s3conf'])
        is_script_file = False
    else:
        task = LocalPathTask(source)
        is_script_file = False

    meta_type = options.get('meta_type')
    if meta_type == 'content':
        return task, is_script_file and open(source, 'r').read() or source
    elif meta_type == 'hash':
        return task, md5(is_script_file and open(source, 'r').read() or source).hexdigest()
    else:
        return task
