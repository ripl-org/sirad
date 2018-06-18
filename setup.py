from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="sirad",
    version="0.1",
    description="Secure Infrastructure for Research with Administrative Data",
    install_requires=requirements,
    packages=find_packages(),
    scripts=["scripts/sirad"]
)
