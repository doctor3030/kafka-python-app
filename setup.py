from setuptools import setup, find_packages

setup(
    name='kafka-app',
    version="0.0.6",
    author="Dmitry Amanov",
    author_email="",
    description="kafka application class",
    long_description="",
    long_description_content_type="",
    url="https://github.com/doctor3030/kafka-app.git",
    classifiers=[
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
