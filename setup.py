#!/usr/bin/env python
from setuptools import setup


options = dict(
    name='luigi-extensions',
    version='0.0.1',
    description='Some luigi enhancement used during work.',
    author='Jerry Zhang',
    author_email='hui.calife@gmail.com',
    url='https://github.com/zh012/luigi-extensions.git',
    packages=['luigi_extensions'],
    package_data={
        'luigi_extensions': [
            'client.cfg.tmpl',
        ],
    },
    license='MIT',
    zip_safe=False,
    platforms='any',
    install_requires=[
        'luigi',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
)

setup(**options)
