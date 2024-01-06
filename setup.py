from setuptools import setup, find_packages

# Read requirements.txt and remove any comments
with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    requirements = [r.strip() for r in requirements if not r.startswith('#')]

setup(
    name='webull_options',
    version='0.3.3',
    packages=find_packages(),
    install_requires=requirements,
)