
# -*- coding: utf-8 -*-

# DO NOT EDIT THIS FILE!
# This file has been autogenerated by dephell <3
# https://github.com/dephell/dephell

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ''

setup(
    long_description=readme,
    name='entwiner',
    version='0.1.0',
    description='Build, use, and share routable transportation graphs using common geospatial data.',
    python_requires='==3.*,>=3.6.0',
    project_urls={"repository": "https://github.com/nbolten/entwiner"},
    author='Nick Bolten',
    author_email='nbolten@gmail.com',
    license='MIT',
    entry_points={"console_scripts": ["entwiner = entwiner.cli:entwiner"]},
    packages=['entwiner', 'entwiner.geopackage', 'entwiner.graphs'],
    package_dir={"": "."},
    package_data={},
    install_requires=['click==7.*,>=7.0.0', 'fiona==1.*,>=1.8.13', 'geomet==0.*,>=0.2.1', 'networkx==2.*,>=2.4.0', 'pyproj==2.*,>=2.4.2', 'shapely==1.*,>=1.6.4'],
    extras_require={"dev": ["black==19.*,>=19.10.0.b0", "dephell==0.*,>=0.8.3", "pre-commit==1.*,>=1.20.0", "pytest==5.*,>=5.2.0", "pytest-cov==2.*,>=2.10.1"]},
)
