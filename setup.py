# -*- coding: utf-8 -*-
from setuptools import setup

# origin author
__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT, see LICENSE file"
__contributors__ = "Sebastiaan Huber"
# modified
__author__ = "Jason Eu"
__modifier__ = "Jason Eu"

about = {}
with open('kiwipy/version.py') as f:
    exec(f.read(), about)

setup(
    name="kiwipy",
    version=about['__version__'],
    description='A python remote communications library',
    long_description=open('README.rst').read(),
    url='https://github.com/unkcpz/kiwipy.git',
    author='Jason Eu',
    author_email='morty.yu@yahoo.com',
    license=__license__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='communication messaging rpc broadcast',
    install_requires=[
        'shortuuid',
    ],
    extras_require={
        'rmq': [
            'pika>=1.0.0b2',
            'aio_pika',
            'tornado>5',
            'pyyaml>=3.13'
        ],
        'dev': [
            'pip',
            'pre-commit',
            'pytest>=4',
            'pytest-cov',
            'asynctest',
            'ipython',
            'twine',
            'yapf',
            'prospector',
            'pylint',
        ],
    },
    packages=['kiwipy', 'kiwipy.rmq'],
    test_suite='test')
