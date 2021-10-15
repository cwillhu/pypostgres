from setuptools import setup, find_packages

setup(
    name='pypostgres',
    version='0.1.0',
    packages=find_packages(include=['pypostgres', 'pypostgres.*'])
)
