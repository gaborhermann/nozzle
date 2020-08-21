import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nozzle",
    version="0.0.1",
    author="Gabor Hermann",
    author_email="code@gaborhermann.com",
    description="Lightweight Python pipeline runner that can execute tasks respecting dependencies.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gaborhermann/nozzle",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)
