import luigi
import luigi.format
import luigi.configuration


def guess_format(f):
    if f.endswith('.gz'):
        return luigi.format.Gzip


class ConfiguredObject(object):
    def __init__(self, conf, *args, **kwargs):
        if isinstance(conf, basestring):
            self.conf = dict(luigi.configuration.get_config().items(conf))
        else:
            self.conf = conf

        if hasattr(self, '_init_'):
            self._init_(*args, **kwargs)


class LocalPathTask(luigi.ExternalTask):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path, format=guess_format(self.path))


class Source(object):
    def __init__(self, uri, **options):
        self.options = options
        self.value = None
        self.task = None

        if '://' in uri:
            self.provider = uri.split('://', 1)[0]
        else:
            self.provider = 'file'
        getattr(self, 'parse_' + self.provider)(uri, options.get(self.provider, {}))

    def parse_redshift(self, uri, opts):
        from . import redshift
        try:
            self.value = open(uri[11:], 'r').read()
        except:
            self.value = uri
        params = {'select': self.value}
        params.update(opts)
        self.task = redshift.UnloadToS3(**params)

    def parse_s3(self, uri, opts):
        from . import s3
        self.value = uri
        params = {'path': self.value}
        params.update(opts)
        self.task = s3.S3PathTask(**params)

    def parse_file(self, uri, opts):
        if '://' in uri:
            self.value = uri.split('://', 1)[1]
        else:
            self.value = uri
        self.task = LocalPathTask(self.value)
