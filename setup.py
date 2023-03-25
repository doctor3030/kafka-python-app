from setuptools import setup, find_packages
import pathlib

# The directory containing this file
ROOOT = pathlib.Path(__file__).parent

# The text of the README file
README = (ROOOT / "README.md").read_text()

setup(
    name='kafka_python_app',
    version="0.2.6",
    author="Dmitry Amanov",
    author_email="dmitry.amanov@gmail.com",
    description="kafka application endpoint",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/doctor3030/kafka-python-app.git",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pydantic~=1.8.2",
        "setuptools~=57.0.0",
        "kafka-python~=2.0.2",
    ]
)
