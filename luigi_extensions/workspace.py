import os
from hashlib import md5
import luigi
from . import grocery, s3


class BaseTask(luigi.Task):
    s3_work_dir = None
    source_meta = None

    name = luigi.Parameter()
    source = luigi.Parameter()
    s3conf = luigi.Parameter(default='s3')
    rsconf = luigi.Parameter(default='redshift-sports')
    skip_fingerprint_check = luigi.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

        self.workspace = s3.S3Workspace(self.s3conf, os.path.join(self.s3_work_dir, self.name))

        if self.source:
            self.source_meta = grocery.Source(self.source, **self.get_source_params())

        fingerprint = self.get_fingerprint()
        marker = self.workspace.get_target('fingerprint')
        if not marker.exists():
            with marker.open('w') as fp:
                fp.write(fingerprint)
        elif not self.skip_fingerprint_check:
            if marker.open().read() != fingerprint:
                raise RuntimeError('Task <{} {}> is started with wrong fingerprint.'.format(self.__class__.__name__, self.name))

    def get_fingerprint(self):
        return md5(self.source_meta.value).hexdigest()

    def get_source_params(self):
        return {
            'redshift': {
                'rsconf': self.rsconf,
                's3conf': self.s3conf,
                's3path': self.workspace.get_path('unloaded_input')
            },
            's3': {
                's3conf': self.s3conf
            }
        }

    def requires(self):
        return self.source_meta.task
