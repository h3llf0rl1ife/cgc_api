from distutils.core import setup

requires = (
    'flask==0.12.2',
    'flask-restful==0.3.6'
)

setup(
    name='Centralisation Gestion Commerciale',
    version='0.1',
    packages=['cgc_api',],
    install_requires=requires,
)

# Install using "pip install -e ."