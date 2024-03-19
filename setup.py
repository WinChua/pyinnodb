# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="pyinnodb",
    version="0.1.0",
    description="A parser for InnoDB file formats, in Python",
    long_description=readme,
    author="WinChua",
    author_email="winchua@foxmail.com",
    url="https://github.com/WinChua/pyinnodb",
    license=license,
    packages=find_packages(exclude=("tests", "docs")),
)
