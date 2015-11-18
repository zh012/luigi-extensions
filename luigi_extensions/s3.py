# -*- coding: utf-8 -*-
from contextlib import contextmanager
from itertools import chain
import json
import luigi
from luigi import configuration, s3, format


class ManifestedInputStream(object):
    def __init__(self, target):
        self._target = target
        self._fps = None

    @property
    def fps(self):
        if self._fps is None:
            with self._target.open('r') as mf:
                manifest = json.loads(mf.read())
            urls = [e['url'] for e in manifest['entries']]
            self._fps = [s3.S3Target(u, guess_format(u), client=self._target.fs).open('r') for u in urls]
        return self._fps

    def close(self):
        if self._fps is not None:
            for fp in self._fps:
                fp.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        return chain(*self.fps)


class S3Target(s3.S3Target):
    def open(self, mode=None):
        if mode is None and self.path.endswith('manifest'):
            return ManifestedInputStream(self)
        return super(S3Target, self).open(mode or 'r')


class S3ConfigMixin(object):
    def get_s3_config(self):
        return dict(configuration.get_config().items(self.s3conf))

    def get_s3_client(self):
        return s3.S3Client(**self.get_s3_config())

    def get_s3_target(self, path, format=None):
        return S3Target(path, format, self.get_s3_client())


class S3WorkspaceMixin(S3ConfigMixin):
    s3workspace = None
    s3format = format.Gzip

    def get_s3_path(self, path):
        if not path.startswith('s3://'):
            path = '/'.join([self.s3workspace.rstrip('/'), path.lstrip('/')])
        return path

    def get_s3_target(self, path, format=None):
        return super(S3WorkspaceMixin, self).get_s3_target(self.get_s3_path(path), format or self.s3format)

    def destory(self):
        self.get_s3_target('').remove()


def guess_format(f):
    if f.endswith('.gz'):
        return format.Gzip
    return format.Text


@contextmanager
def open_manifest(target, fmt=None):
    manifest = json.loads(target.open('r').read())
    urls = [e['url'] for e in manifest['entries']]
    fps = [s3.S3Target(u, guess_format(u), client=target.fs).open('r') for u in urls]
    try:
        yield chain(*fps)
    finally:
        for fp in fps:
            try:
                fp.close()
            except:
                pass


class S3PathTask(S3ConfigMixin, luigi.ExternalTask):
    path = luigi.Parameter()
    s3conf = luigi.Parameter(default='s3')

    def output(self):
        return self.get_s3_target(self.path, format=guess_format(self.path))
