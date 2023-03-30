try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()    

setup(name='cmldask',
      version='1.0',
      install_requires=required,
      packages=find_packages(),
      author='Joseph Rudoler',
      author_email=['jrudoler56@gmail.com', 'kahana-sysadmin@gmail.com'],
      py_modules=['cmldask'],
      )
