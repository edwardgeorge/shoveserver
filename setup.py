from setuptools import setup, find_packages

setup(
    name='shoveserver',
    version='0.1',
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=['eventlet>=0.9.6'],
)

