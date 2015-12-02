from setuptools import setup, find_packages
import os

version = '1.0.0'

setup(
    name='org.bccvl.movelib',
    version=version,
    description="Data Mover base library",
    # long_description=open("README.txt").read() + "\n" +
    #                  open(os.path.join("docs", "HISTORY.txt")).read(),
    # Get more strings from
    # http://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        "Framework :: Plone",
        "Programming Language :: Python",
    ],
    keywords='',
    author='',
    author_email='',
    url='http://svn.plone.org/svn/collective/',
    license='GPL',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    namespace_packages=['org', 'org.bccvl'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'setuptools',
    ],
    extras_require={
        'scp': ['paramiko', 'scp'],
        'swift': ['python-swiftclient', 'python-keystoneclient'],
        'http': ['requests'],
    },

)
