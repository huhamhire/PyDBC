import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name="PyDBC",
    version="0.0.1",
    author="Hamhire Hu",
    author_email="me@huhamhire.com",
    description=("A simple package providing lightweight DDL and DML tools for "
                 "managing different databases"),
    license="MIT",
    keywords="database SQL DDL DML management",
    url="https://github.com/huhamhire/PyDBC",
    packages=["pydbc",
              'pydbc.dialect'],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 1 - Planning",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Utilities",
    ],
    zip_safe=False,
)
