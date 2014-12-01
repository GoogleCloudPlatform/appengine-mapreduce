from distutils.core import setup

setup(
    name="mapreduce",
    version="1.0.0",
    packages=["mapreduce"],
    author="VendAsta",
    author_email="jcollins@vendasta.com",
    keywords="google app engine mapreduce data processing",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    license="Apache License 2.0",
    description="Enable MapReduce style data processing on App Engine",
    install_requires=[
            "GoogleAppEngineCloudStorageClient >= 1.9.5",
            "GoogleAppEnginePipeline >= 1.9.5.1",
            "Graphy >= 1.0.0",
            "simplejson >= 3.6.5",
            "mock >= 1.0.1",
            "mox >= 0.5.3",
        ]
)
