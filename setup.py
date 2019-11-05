from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

with open("sirad/VERSION") as f:
    version = f.read().strip()

setup(
    name="sirad",
    author="Mark Howison",
    author_email="mhowison@ripl.org",
    version=version,
    url="https://github.com/ripl-org/sirad",
    description="Secure Infrastructure for Research with Administrative Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: Free for non-commercial use",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering"],
    provides=["sirad"],
    install_requires=[
        "openpyxl",
        "pandas",
        "PyYAML",
        "usaddress"],
    packages=find_packages(),
    package_data={"sirad": ["VERSION"]},
    scripts=["scripts/sirad"]
)
