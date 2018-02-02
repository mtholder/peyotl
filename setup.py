#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Based on template found at: https://github.com/kennethreitz/setup.py
from setuptools import setup, find_packages

readme = open('README.md').read()
license = open('License.txt').read()
reqs = [i.strip() for i in open('requirements.txt').readlines() if i.strip()]
scripts = ['scripts/clipeyotl.py',
          ]
classifiers = ['Development Status :: 3 - Alpha',
               'Intended Audience :: Science/Research',
               'Intended Audience :: Developers',
               'Natural Language :: English',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.3',
               'Topic :: Scientific/Engineering :: Bio-Informatics',
               ]
setup(
    name='peyotl',
    version='0.1.4dev',  # sync with __version__ in peyotl/__init__.py
    description='Library for interacting with Open Tree of Life resources',
    long_description=readme,
    url='https://github.com/OpenTreeOfLife/peyotl',
    license=license,
    author='Emily Jane B. McTavish and Mark T. Holder and see CONTRIBUTORS.txt file',
    py_modules=['peyotl'],
    install_requires=reqs,
    packages=['peyotl'],
    classifiers=classifiers,
    include_package_data=True,
    scripts=scripts,
    test_suite='tests',
    zip_safe=False
)

