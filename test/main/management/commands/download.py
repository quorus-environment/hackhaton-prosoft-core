import os
import json

import xmltodict
import tarfile

from django.conf import settings
from django.core.management import BaseCommand

from main.models import (
    Client,
    Controller,
    Server,
    Werehouse
)

class ErrorData(Exception):
    pass

class Command(BaseCommand):
    """Класс загрузки данных в бд.
    
    Нужно прописать в консоли команду из переменной {help}
    """
    path_to_controllers = f'{settings.BASE_DIR}/static/data/data_for_parse'
    save_to = f'{settings.BASE_DIR}/res_parse/'
    list_controllers = []
    path_data_controller = 'res_parse'
    help = 'Попробуй python manage.py download'

    def handle(self, *args, **kwargs):
        """Точка входа при запуске из консоли тут."""

        self.__parse_controllers()
        self.__parse_data_controllers()
        self.__get_data_file()

    def __parse_controllers(self):
        list_files = os.listdir(self.path_to_controllers)
        for idx_file in range(len(list_files)):
            file = list_files[idx_file]
            if not file.endswith('tar'):
                continue

            file_to_save = f'{self.path_to_controllers}/res_parse_controller_{idx_file}'
            self.list_controllers.append(file_to_save)
            tar = tarfile.open(f'{self.path_to_controllers}/{file}', "r:")
            tar.extractall(file_to_save)
            tar.close()

    def __parse_data_controllers(self):
        for path_controller in self.list_controllers:
            list_files = os.listdir(path_controller)
            for idx_file in range(len(list_files)):
                file = list_files[idx_file]
                if not file.endswith("tar.gz"):
                    continue

                file_to_save = f'{path_controller}/{self.path_data_controller}'
                tar = tarfile.open(f'{path_controller}/{file}', "r:gz")
                tar.extractall(file_to_save)
                tar.close()
    
    def __get_data_file(self):
        for path_controller in self.list_controllers:
            base_path_place = f'{path_controller}/{self.path_data_controller}'
            self.load_client_data(base_path_place)
            self.load_server_data(base_path_place)
            self.load_werehouse_data(base_path_place)

    def load_client_data(self, base_file):
        """Загрузка клиента."""

        with open(f'{base_file}/etc/KC/iec104_req.xml') as fd:
            req = xmltodict.parse(fd.read())
        result = {
            'slaves': []
        }
        try:
            each_data = req['NODES']['SLAVES']['SLAVE']
        except TypeError:
            self.__save_data(base_file, result, 'client')
            return
        
        if type(each_data) == list:
            for data in each_data:
                name = data['@NAME']
                ip = data['DATA_SOURCES']['DS']['@IP_ADDRESS']
                points = self.__get_points(data['POINTS']['POINT'])
                result['slaves'].append(self.__slave(name, ip, points))
        else:
            name = each_data['@NAME']
            ip = each_data['DATA_SOURCES']['DS']['@IP_ADDRESS']
            points = self.__get_points(each_data['POINTS']['POINT'])
            result['slaves'].append(self.__slave(name, ip, points))
        
        self.__save_data(base_file, result, 'client')
        self.stdout.write(self.style.SUCCESS('Данные Client загружены'))

    def __get_points(self, data):
        result = []
        for item in data:
            result.append(
                self.__point_client(item)
            )
        return result

    def __point_client(self, item):
        return {
            'name': item['@NAME'],
            'address': item['@ADDRESS']
        }

    def __slave(self, name, ip, points):
        return {
            'name': name,
            'ip': ip,
            'points': points
        }

    def __save_data(self, base_file, data, type):
        file_name = base_file.split('/')[-2]
        with open(f'{self.save_to}{file_name}_{type}.json', 'w') as f:
            json.dump(data, f, ensure_ascii=False)

    def load_server_data(self, base_file):
        """Загрузка сервера."""

        with open(f'{base_file}/etc/KC/iec104_serv.xml') as fd:
            req = xmltodict.parse(fd.read())
        result = {'masters': []}
        try:
            each_data = req['NODES']['MASTERS']['MASTER']
            if each_data is None:
                raise TypeError
        except TypeError:
            self.__save_data(base_file, result, 'server')
            return

        if type(each_data) == list:
            for data in each_data:
                name = data['@NAME']
                ip = data['PARAMS']['@IP_ADDRESS']
                points = self.__get_points_server(data['POINTS']['POINT'])
                result['masters'].append(self.__master(name, ip, points))
        else:
            name = each_data['@NAME']
            ip = each_data['PARAMS']['@IP_ADDRESS']
            points = self.__get_points_server(each_data['POINTS']['POINT'])
            result['masters'].append(self.__master(name, ip, points))
        self.__save_data(base_file, result, 'server')
        self.stdout.write(self.style.SUCCESS('Данные Server загружены'))

    def __get_points_server(self, data):
        result = []
        for item in data:
            result.append(
                self.__point_server(item)
            )
        return result

    def __point_server(self, data):
        return {
            'name': self.__parse_name_server(data['@NAME']),
            'client': self.__parse_client_server(data['@NAME']),
            'address': data['@ADDRESS']
        }

    def __parse_name_server(self, data):
        return data.split('.')[-1]

    def __parse_client_server(self, data):
        return data.split('.')[-2]

    def __master(self, name, ip, points):
        return {
            'name': name,
            'ip': ip,
            'points': points
        }
    
    def load_werehouse_data(self, base_file):
        """Загрузка werehouse."""

        with open(f'{base_file}/etc/KC/warehouse.xml') as fd:
            req = xmltodict.parse(fd.read())
        result = {
            'points': [],
            'formuls': []
        }
        each_data = req['KERNEL']['POINTS']['POINT']
        for data in each_data:
            if '@FORMULA' in data.keys():
                result['formuls'].append(
                    self.__werehouse_point(data)
                )
                continue
            try:
                result['points'].append(
                    self.__point_werehous(data['@NAME'])
                )
            except ErrorData:
                continue
        self.__save_data(base_file, result, 'werehouse')
        self.stdout.write(self.style.SUCCESS('Данные Werehouse загружены'))

    def __point_werehous(self, data):
        parse_name = data.split('.')
        if not '-' in parse_name[-1]:
            raise ErrorData
        if parse_name[0].split()[-1] == 'Serv':
            raise ErrorData
        return {
            'name': parse_name[-1],
            'client': parse_name[-2]
        }

    def __werehouse_point(self, data):
        return {
            'name': data['@NAME'].split('.')[-1],
            'client': data['@NAME'].split('.')[-2],
            'ref': self.__ref_point(data)
        }

    def __ref_point(self, data):
        result = []
        split_d = data['@FORMULA'].split('\',#\'')
        for item in split_d:
            result.append(
                {
                    'name': item.split('.')[-1].split('\'')[0],
                    'client': item.split('.')[-2]
                }
            )
        return result
