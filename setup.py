#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='django-cyclebufferfield',
    description="Field to manage Django fields in a fixed-size ring buffer.",
    version='0.1',
    url='http://code.playfire.com/',

    author='Playfire.com',
    author_email='tech@playfire.com',
    license='BSD',

    packages=find_packages(),
)
