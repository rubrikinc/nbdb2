"""
AnomalyDB dependencies
"""
import logging
import os

from setuptools import setup, find_packages

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

install_requires = [str(requirement.requirement) for requirement in requirements]

setup(name='nbdb',
      version='0.1',
      author='Rubrik',
      license='MIT',
      install_requires=install_requires,
      packages=find_packages(),
      package_data={'': ['ml.json',
                         'logger.conf',
                         'settings_prod.yaml',
                         'settings.yaml']},
      python_requires='>=3',
      zip_safe=False)
