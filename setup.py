from setuptools import setup, find_packages
import sys, os

version = '0.1.0'

setup(name='Notanormous',
      version=version,
      description="Certainly not an ORM for MongoDB, because that would be absurd.",
      long_description="",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='mongodb orm database nosql',
      author='Isaac Csandl <http://isaaccsandl.com/>',
      author_email='ic@isaaccsandl.com',
      url='',
      license='GPL v3.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'pymongo>=2.7.2',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
