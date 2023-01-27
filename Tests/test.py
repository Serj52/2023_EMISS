import json
import unittest
from config import Config as cfg
from producer import Rabbit
import os




class Tests:
    def __init__(self):
        self.rabbit = Rabbit()
        self.ipc = {'file_data.json': False, 'ipc_service.xlsx': False, 'response.json': False}
        self.okpd = {'file_data.json': False, '57608.xml': False, 'response.json': False}
        self.okved = {'file_data.json': False, '57609.xml': False, 'response.json': False}

    def send_message(self):
        """Отправка запросов в очередь"""
        self.rabbit.producer_queue(queue_name=cfg.queue_request, data_path='IPC.json')
        # self.rabbit.producer_queue(queue_name=cfg.queue_request, data_path='OKPD.json')
        # self.rabbit.producer_queue(queue_name=cfg.queue_request, data_path='OKVED.json')

    def run_test(self):
        self.test_request(self.ipc, cfg.processed_files_IPC, 'IPC')
        self.test_request(self.okpd, cfg.processed_files_OKPD, 'OKPD')
        self.test_request(self.okved, cfg.processed_files_OKVED, 'OKVED')

    def test_request(self, templates, processed_files, type_request):
        """
        Проверка налия фалов в папке processed_files после обработки запроса
        Перед самими нужно очистить processed_files
        """
        #Наличие файла response.json
        response_exist = False
        # Наличие тегов header и body в response.json
        header_body = False
        found_files = True
        for template in templates:
            for file in os.listdir(processed_files):
                # Проверка наличия файлов по итогам обработки запроса
                if template == file:
                    templates[template] = True
                if 'response.' in file:
                    response_exist = True
                    with open(os.path.join(cfg.processed_files_IPC, file), 'r', encoding='utf-8') as response:
                        data = json.load(response)
                    for tag in data:
                        if tag == 'header' or tag == 'body':
                            header_body = True
                            # Проверка тегов на заполненность данными
                            for key in data[tag]:
                                assert data[tag][key] != ''

        for template in templates:
            if templates[template] is False:
                print(f'Test {type_request}. {template} NOT FOUND')
                found_files = False

        if response_exist is True and header_body is True and found_files is True:
            print(f'Test {type_request} passed')
        else:
            print(f'Test {type_request} ERROR')



if __name__ == '__main__':
    # Tests().send_message()
    Tests().send_message()


