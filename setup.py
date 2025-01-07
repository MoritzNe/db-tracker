from setuptools import setup, find_packages

setup(
    name="your-package-name",  # Replace with your package name
    version="0.1.0",           # Initial version
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python library for database state snapshots and diffs.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/<your-username>/<your-repo>",  # GitHub repo URL
    packages=find_packages(),
    install_requires=[
        "pytest",
        "sqlalchemy",
        "syrupy"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
