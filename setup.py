import setuptools

setuptools.setup(
    name="#{Build.DefinitionName}#",
    version="#{version}#", 
    description="Python #{Build.DefinitionName}# Package",
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'},
    install_requires=[
        'pandas',
        'numpy',
        'datetime',
        'py2neo',
    ]
)
