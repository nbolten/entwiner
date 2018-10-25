from setuptools import setup

setup(
    name="entwiner",
    version="0.1",
    py_modules=["entwiner"],
    install_requires=[
        "Click",
        "pyproj",
    ],
    entry_points="""
        [console_scripts]
        entwiner=entwiner.__main__:entwiner
    """,
)
