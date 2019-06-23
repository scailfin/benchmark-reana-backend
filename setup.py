from setuptools import setup


readme = open('README.rst').read()

install_requires=[
    'benchmark-templates==0.1.2',
    'reana-client==0.5.0'
]


tests_require = [
    'benchmark-multiprocess>=0.1.1',
    'coverage>=4.0',
    'coveralls',
    'nose'
]


extras_require = {
    'docs': [
        'Sphinx',
        'sphinx-rtd-theme'
    ],
    'tests': tests_require,
}


setup(
    name='benchmark-reana',
    version='0.1.0',
    description='REANA Workflow Engine for Reproducible Benchmark Templates',
    long_description=readme,
    long_description_content_type='text/x-rst',
    keywords='reproducibility benchmarks data analysis',
    url='https://github.com/scailfin/benchmark-reana-backend',
    author='Heiko Mueller',
    author_email='heiko.muller@gmail.com',
    license='MIT',
    packages=['benchreana'],
    include_package_data=True,
    test_suite='nose.collector',
    extras_require=extras_require,
    tests_require=tests_require,
    install_requires=install_requires
)
