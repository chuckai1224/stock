from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext
import numpy

ext_modules = [
    Extension("comm",  ["comm.pyx"],include_dirs=[numpy.get_include()])
    #Extension("mymodule2",  ["bt.pyx"]),
]
for e in ext_modules:
    e.cython_directives = {'language_level': "3"}
setup(
    name = 'comm test',
    cmdclass = {'build_ext': build_ext},
    ext_modules = ext_modules
)
