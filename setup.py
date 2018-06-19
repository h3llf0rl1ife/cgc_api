from distutils.core import setup

requires = (
    'flask>=1.0.1',
    'flask-restful>=0.3.6',
    'python-dateutil>=2.7.2',
    # 'pymssql==2.1.3' install from .wheel instead
)

setup(
    name='cgc_api',
    version='0.3',
    packages=['cgc_api'],
    install_requires=requires,
)

# Install using "pip install -e ."
