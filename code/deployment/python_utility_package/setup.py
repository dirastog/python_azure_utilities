from setuptools import setup, find_packages

data = dict(
    name="utility_package",
    version="0.1",
    install_requires=[],
    data_files=[],
    packages=find_packages(),
)

if __name__ == '__main__':
    setup(**data)
