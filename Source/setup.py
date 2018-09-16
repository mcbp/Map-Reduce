from cx_Freeze import setup, Executable
import sys

include_files = []
includes = []
excludes = []
packages = ['decimal'] # leave decimal, you shouldn't need to add anything else.

setup(
    name='',
    version='1.0.0',
    description='',
    author='',
    author_email='',
    options={'build_exe': {
        'excludes': excludes,
        'packages': packages,
        'include_files': include_files
    }},
    executables=[
        Executable('MapReduce.py'),
    ]
)