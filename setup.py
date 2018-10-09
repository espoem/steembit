import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "./README.md"), "r") as f:
    readme = f.read()

requirements = ["Click", "beem", "PTable"]

setup(
    name="steembit",
    version="0.0.2",
    description="Simple CLI tool for Steem users",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    author="Martin Šmíd",
    author_email="martin.smid94@seznam.cz",
    license="MIT",
    keywords="steem cli",
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ],
    entry_points="""
        [console_scripts]
        steembit=steembit.steembit:cli
    """,
)
