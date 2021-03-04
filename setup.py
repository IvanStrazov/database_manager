# utf-8
# Python 3.7
# 2020-11-16


import setuptools
import database_manager


setuptools.setup(
    name="database_manager",
    version=database_manager.__version__,
    packages=setuptools.find_packages(),
    description="DataBase Manager for Potential Team",
    author="Ivan Strazov",
    author_email="IPStrazov@sberbank.ru",
    url="https://stash.delta.sbrf.ru/projects/POTPLAN/repos/database_manager"
)
