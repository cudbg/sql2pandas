#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages

setup(name="sql2pandas",
      version="0.0.02",
      description="Turn SQL into Pandas Statements",
      license="MIT",
      include_package_data = True,      
      packages = find_packages(),
      package_dir = {'sql2pandas' : 'sql2pandas'},
      scripts = [],
      package_data = { 'sql2pandas' : ['sql2pandas/data/*'] },
      install_requires = [
        'parsimonious', 'pandas', 'numpy',
        'python-dateutil', 'pytest'
      ],
      keywords= "database pandas engine compiler")
