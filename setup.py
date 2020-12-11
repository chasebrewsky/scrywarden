from setuptools import setup, find_packages

with open("README.md", "r") as file:
    README = file.read()

setup(
    name='scrywarden',
    version='0.1.1',
    author='Chase Brewer',
    author_email='chasebrewsky@gmail.com',
    description="Detect anomalies in datasets using behavioral modeling",
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/chasebrewsky/scrywarden',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(),
    install_requires=[
        'click==7.1.*',
        'orjson==3.4.*',
        'PyYAML==5.3.*',
        'pandas==1.1.*',
        'psycopg2==2.8.*',
        'SQLAlchemy==1.3.*',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'scrywarden = scrywarden.cli:main',
        ]
    }
)
