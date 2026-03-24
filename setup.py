# setup.py
from setuptools import setup, find_packages

setup(
    name="sistema-maconico",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "Flask==2.3.3",
        "psycopg2-binary==2.9.9",
        "python-dotenv==1.0.0",
        "Werkzeug==2.3.7",
        "openpyxl==3.1.2",
        "reportlab==4.0.7",
        "markdown==3.5.1",
        "gunicorn==21.2.0",
    ],
)