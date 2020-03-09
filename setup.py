from setuptools import find_packages, setup

INSTALL_REQUIRES = [
    'sqlalchemy',
    'matplotlib',
    'protobuf',
    'psycopg2',
    'pillow',
    'pandas',
    'seaborn'
]

ENTRY_POINTS = {
    'console_scripts': [
        'CorrelatR=correlatr.scripts.start_server:main',
        'compile_protos=correlatr.scripts.compile_protos:main'
    ]
}

setup(
    name="CorrelatR",
    version="0.0.1",
    packages=find_packages("src"),
    author='creynolds1',
    description = "Python Server for CorrelatR",
    install_requires = INSTALL_REQUIRES,
    package_dir={'': 'src'},
    entry_points=ENTRY_POINTS
)