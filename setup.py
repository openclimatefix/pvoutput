from setuptools import setup, find_packages

setup(
    name='pvoutput',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'tables',
        'pandas',
        'matplotlib',
        'jupyter',
        'urllib3',
        'requests']
)
