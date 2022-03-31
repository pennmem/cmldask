try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(name='cmldask',
      version='0.1',
      packages=find_packages(),
      author='Joseph Rudoler',
      author_email=['jrudoler56@gmail.com', 'kahana-sysadmin@gmail.com'],
      py_modules=['cmldask'],
      )
