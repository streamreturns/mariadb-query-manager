from setuptools import setup
from MariaDbQueryManager import __version__

setup(name='MariaDbQueryManager',
      version=__version__,
      author='Byeongho Kang',
      author_email='byeongho.kang@yahoo.com',
      description='MariaDB and MySQL compatible Database Query Manager',
      packages=['MariaDbQueryManager', ],
      long_description=open('README.md', encoding='utf-8').read(),
      zip_safe=False,
      include_package_data=True,
      )
