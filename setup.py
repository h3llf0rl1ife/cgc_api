from distutils.core import setup

requires = (
    'flask==1.0.1',
    'flask-restful==0.3.6',
    #'pymssql==2.1.3'
)

setup(
    name='cgc_api',
    version='0.1',
    packages=['cgc_api'],
    install_requires=requires,
)

# Install using "pip install -e ."