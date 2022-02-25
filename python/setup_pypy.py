"""
AnomalyDB dependencies for pypy, its used to run certain components
which can be sped up if they are written in pure python and/or are pypy
compatible. All packages part of setup.py are included, packages not
compatible are added in EXCLUDED_PACKAGES.
please check http://packages.pypy.org/
"""
import logging
import os

from setuptools import setup, find_packages

EXCLUDED_PACKAGES = [
    'confluent-kafka',
    'cryptography',
    'lz4',
    'numpy',
    'orjson',
    'pandas',
    'pynacl',
    'scipy',
    'sklearn',
    'sshtunnel',
    'tdigest'  # according to google, tdigest pypy perf is bad so exclude it
]

logger = logging.getLogger(__name__)

try:
    # pip >=20
    from pip._internal.network.session import PipSession
    from pip._internal.req import parse_requirements
except ImportError:
    logger.error("pip version must be >=20")
    raise

requirements = parse_requirements(os.path.join(
    os.path.dirname(__file__), 'requirements.txt'), session=PipSession())
python_install_requires = [str(requirement.requirement) for requirement in requirements]

pypy_install_requires = []
for package in python_install_requires:
    if package.split("==")[0] not in EXCLUDED_PACKAGES:
        pypy_install_requires.append(package)

setup(name='nbdb',
      version='0.1',
      author='Rubrik',
      license='MIT',
      install_requires=pypy_install_requires,
      packages=find_packages(),
      package_data={'': ['ml.json',
                         'logger.conf',
                         'settings_prod.yaml',
                         'settings.yaml']},
      python_requires='>=3',
      zip_safe=False)
