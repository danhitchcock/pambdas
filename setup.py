"""
Setup for the module
"""
from setuptools import setup
import codecs
import os.path


def read(rel_path):
    """
    Opens a path file
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    """
    Pulls version from module
    """
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(
    name="pambdas",
    version=get_version("pambdas/__init__.py"),
    description="A pure-Python DataFrame implementation, with a similar API to Pandas.",
    author="Daniel Hitchcock",
    packages=["pambdas"],
)
