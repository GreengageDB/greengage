from setuptools import setup

setup(
    name="PyGreSQL-compat",
    version="1.0.0",
    packages=["pygresql"],
    install_requires=["PyGreSQL>=5.2,<6"],
)

