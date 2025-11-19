""" Setup file """
from setuptools import setup
from setuptools import find_packages
from glob import glob
from os.path import splitext
from os.path import basename
import versioneer

setup(
    name = 'vsti-vapi-modelo-predictivo-apis-dev',
    description = 'Modelo descriptivo para predecir la transaccionalidad y fallas de las APIs de la EVC Servicios de Integracin',
    url = 'https://GrupoBancolombia@dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_git/vsti-vapi-modelo-predictivo-apis-dev',
    author = 'lrivera, anpolo',
    author_email = 'lrivera@bancolombia.com.co',
    license = '...',
    packages = find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    python_requires='>=3.9.12',
    entry_points = {
        'console_scripts': ['vsti_vapi_modelo_predictivo_apis_dev = vsti_vapi_modelo_predictivo_apis_dev.ejecucion:main']
    },
    install_requires = [
        'future_fstrings',
        'orquestador2>=1.3.2'
    ],
    include_package_data = True,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
