from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='tg_parser',
    version='0.1',
    packages=find_packages(),
    install_requires=requirements,
)

# pip install -e /home/wudmc/PycharmProjects/tg_parser
