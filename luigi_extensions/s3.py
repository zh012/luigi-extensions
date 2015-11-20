# -*- coding: utf-8 -*-
from itertools import chain
import json
import luigi
from luigi import s3
from .grocery import guess_format, ConfiguredObject


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


class S3(ConfiguredObject):
    def get_client(self):
        return s3.S3Client(**self.conf)

    def get_target(self, path, fmt=None):
        return S3Target(path, fmt or guess_format(path), self.get_client())


class S3Workspace(S3):
    def _init_(self, root):
        self.root = root.rstrip('/')

    def get_path(self, path):
        if not path.startswith('s3://'):
            path = '/'.join([self.root, path.lstrip('/')])
        return path

    def get_target(self, path, fmt=None):
        return super(S3Workspace, self).get_target(self.get_path(path), fmt)

    def destory(self):
        self.get_target('').remove()

    def write(self, filename, content):
        with self.get_target(filename).open('w') as out:
            out.write(content)

    def touch(self, fingerprint):
        flag = self.get_target('fingerprint')
        if flag.exists():
            if flag.open().read() != fingerprint:
                raise RuntimeError('Workspace already exists with differnt fingerprint.'.format(self.root))
        else:
            with flag.open('w') as out:
                out.write(fingerprint)


class S3PathTask(luigi.ExternalTask):
    path = luigi.Parameter()
    s3conf = luigi.Parameter()

    def output(self):
        return S3(self.s3conf).get_target(self.path)
