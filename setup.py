#!/bin/env python

from distutils.core import setup

name = 'dropboxfs'
version = '0.1'
release = '2'
versrel = version + '-' + release
readme = 'README.rst'
download_url = 'https://github.com/downloads/btimby/fs-dropbox' \
                           '/' + name + '-' + versrel + '.tar.gz'
long_description = file(readme).read()

setup(
    name = name,
    version = versrel,
    description = 'A pyFilesystem backend for the Dropbox API.',
    long_description = long_description,
    author = 'Ben Timby',
    author_email = 'btimby@gmail.com',
    maintainer = 'Ben Timby',
    maintainer_email = 'btimby@gmail.com',
    url = 'http://github.com/btimby/fs-dropbox/',
    download_url = download_url,
    license = 'GPLv3',
    py_modules=['dropboxfs'],
    classifiers = (
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
