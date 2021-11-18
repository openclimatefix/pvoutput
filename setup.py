from setuptools import find_packages, setup

setup(
    name="pvoutput",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pytest",
        "pyyaml",
        "tables",
        "pandas",
        "matplotlib",
        "jupyter",
        "urllib3",
        "requests",
        "beautifulsoup4",
    ],
)
