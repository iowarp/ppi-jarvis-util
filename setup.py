import setuptools

setuptools.setup(
    name="jarvis_util",
    version="0.0.1",
    scripts=['bin/pylsblk'],
    description='Utilities to aid with creating shell scripts within Python.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Luke Logan",
    author_email="llogan@hawk.iit.edu",
    url="https://github.com/scs-lab/jarvis-util",
    packages=setuptools.find_packages(),
    install_requires=[
        'pyyaml',
        'pylint==2.15.0',
        # 'coverage==5.5',
        # 'coverage-lcov==0.2.4',
        'pytest==6.2.5',
        'tabulate'
    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 0 - Pre-Alpha",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: None",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Application Configuration",
    ],
)
