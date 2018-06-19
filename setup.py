from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="sirad",
    version="0.1.2",
    url="https://github.com/riipl-org/sirad",
    description="Secure Infrastructure for Research with Administrative Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: Free for non-commercial use",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering"],
    provides=["agalma"],
    install_requires=[
        "jellyfish",
        "openpyxl",
        "sqlalchemy",
        "PyYAML"],
    packages=find_packages(),
    scripts=["scripts/sirad"]
)
