from setuptools import setup, find_packages

from felon.__version__ import __version__


setup(
    name="felon",
    version=__version__,
    url="https://github.com/arttuperala/felon",
    author="Arttu Perälä",
    author_email="arttu@perala.me",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Topic :: Internet :: WWW/HTTP",
    ],
    packages=find_packages(),
    install_requires=[
        "click==7.1.2,<7.2",
        "kafka-python==2.0.2,<3",
        "psycopg2==2.8.6",
        "requests>=2.25.0,<3",
    ],
    entry_points={
        "console_scripts": ["felon=felon.cli:cli"],
    },
)
