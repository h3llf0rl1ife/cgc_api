from distutils.core import setup

requires = (
    'flask>=1.0',
    'flask-restful>=0.3.6',
    'python-dateutil>=2.7.2',
    'cryptography>=2.3.1',
    'flask-sqlalchemy',
    # 'pymssql==2.1.3' install from .wheel instead
)

setup(
    name='cgc_api',
    version='1.0',
    packages=['cgc_api'],
    install_requires=requires,
)

# Install using "pip install -e ."
