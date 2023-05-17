from setuptools import setup, find_packages
from os.path import basename
from os.path import splitext
from glob import glob

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='generic_consumer',
    version='0.1.0',
    description='Generic Kafka Consumer',
    author='Kelsey Mok',
    author_email='kelseymok@gmail.com',
    url='https://github.com/data-derp/generic_consumer',
    packages=find_packages('src', exclude=('tests')),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    install_requires=required
)
