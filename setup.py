import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='stream-sqlite',
    version='0.0.30',
    author='Department for International Trade',
    author_email='sre@digital.trade.gov.uk',
    description='Python function to extract all the rows from a SQLite database, without loading the entire file into memory or disk',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/stream-sqlite',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    python_requires='>=3.5.0',
    py_modules=[
        'stream_sqlite',
    ],
)
