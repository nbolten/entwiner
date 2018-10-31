from setuptools import setup, find_packages

setup(
    name="entwiner",
    version="0.1",
    py_modules=["entwiner"],
    install_requires=[
        "Click",
        "pyproj",
    ],
    packages=find_packages(),
    entry_points="""
        [console_scripts]
        entwiner=entwiner.__main__:entwiner
    """,
)
