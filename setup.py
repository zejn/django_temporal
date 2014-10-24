
from setuptools import setup, find_packages

setup(name='django_temporal',
      version='0.3',
      description='Temporal database extension for Django',
      author='Gasper Zejn',
      author_email='zejn@kiberpipa.org',
      url='http://github.com/zejn/django_temporal',
      packages=find_packages(exclude=['temporal']),
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Topic :: Database',
          'Framework :: Django',
          'License :: OSI Approved :: BSD License'
        ]
     )
