"""Модуль, который инициализирует класс Config, отвечающий за чтение параметров конфигурации в формате yml"""

import yaml

class Config:
    def __init__(self, path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    #достает из Config нужную часть
    def get_config(self, application):
        return self.__config.get(application)
#
