from __future__ import absolute_import
install_requires = [
    'setuptools',
    'sqlalchemy >= 1.0.10',
    ]

tests_require = [
    'nose >= 1.3.0'
    ]

from setuptools import setup, find_packages


setup(name='graff',
      version='1.0.1',
      description='A graph database built on top of SQLAlchemy',
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "License :: OSI Approved :: BSD License",
      ],
      author="Andrew Pontzen",
      author_email="a.pontzen@ucl.ac.uk",
      license="BSD",
      packages=find_packages(),
      url="https://github.com/apontzen/graff",
      entry_points={},
      include_package_data=True,
      zip_safe=False,
      python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
      install_requires=install_requires,
      tests_require=tests_require,
      test_suite="nose.collector"
      )
