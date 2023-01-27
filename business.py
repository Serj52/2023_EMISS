import logging
from Lib.SELENIUM import Selenium
import logging as lg
from datetime import datetime
import time
import os
from selenium.common.exceptions import TimeoutException
from config import Config as cfg
from Lib.EXCEPTION_HANDLER import WebsiteError, NotFoundElement, FileProcessError, FileNotFound
from bs4 import BeautifulSoup
import re
import json
from Lib.RABBIT import Rabbit
import requests
from pathlib import Path
from EXCEL import Excel
import shutil
import zipfile
import base64



class Business:
    """Класс для построения бизнес процесса при работе с сайтом fedstat.ru"""
    def __init__(self):
        self.web = Selenium()
        self.rabbit = Rabbit()
        self.excel = Excel()
        self.queue_response = None
        self.task_id = None
        self.date_update = None
        self.classifier = None
        self.period = None

    def clean_dir(self, load_dir):
        """
        Очищаю директорию от случайно оставшихся файлов
        """
        for file in os.listdir(load_dir):
            if os.path.isfile(os.path.join(load_dir, file)):
                os.remove(os.path.join(load_dir, file))

    def rosstat_work(self, period, classifer,):
        # Обработка сайта https://rosstat.gov.ru/
        self.clean_dir(cfg.load_dir_IPC)
        loaded_file = self.load_price_service()
        self.excel.excel_to_json(workbook_path=loaded_file, json_path=cfg.template_file_data, date_begin=period)
        self.file_compression(cfg.load_dir_IPC, cfg.processed_files_IPC)
        response = self.create_response(cfg.load_dir_IPC, classifer, cfg.template_response)
        self.rabbit.send_data_queue(self.queue_response, response)
        shutil.copy(response, cfg.processed_files_IPC)
        os.remove(response)
        self.excel.add_data_to_log(self.task_id, classifer, 'Выполнено')

    def fedstat_work(self, url: str, xpath: dict, name_file: str, classifer: str, period: str, load_dir,
                     processed_folder, max_tries=4):
        """
        Обработка запроса
        :param url:электронный адрес сайта
        :param xpath:словарь с набором селекторов
        :param name_file:имя загруженного файла
        :param classifer:'PPI_by_OKPD' or 'PPI_by_OKPD'
        :param period:период формата '2022-03-02'
        :param max_tries:число попыток обрабоать запрос
        """
        while max_tries > 0:
            try:
                #Обработка сайта https://fedstat.ru/
                max_tries -= 1
                self.clean_dir(load_dir)
                self.web.open_site(site_url=url, queue_response=self.queue_response, task_id=self.task_id,
                                   load_dir=load_dir)
                self.web.set_filter(xpath)
                self.web.download_file()
                self.web.refresh_file(name_file, load_dir)
                self.web.close_site()
                response = self.xml_to_json(classifer, period, load_dir, processed_folder)
                self.rabbit.send_data_queue(self.queue_response, response)
                shutil.copy(response, processed_folder)
                os.remove(response)
                self.excel.add_data_to_log(self.task_id, classifer, 'Выполнено')
                return
            except TimeoutException:
                if max_tries == 1:
                    lg.error(f'Данные не загрузились. Осталось попыток {max_tries}')
                    self.web.close_site()
                    self.web.wait(21, 'hours', self.queue_response, self.task_id)
                elif max_tries != 0:
                    lg.error(f'Данные не загрузились. Повторно открываю сайт. Осталось попыток {max_tries}')
                    self.web.close_site()
            except FileNotFound:
                self.web.close_site()
                if max_tries != 0:
                    lg.error(f'Файл не скачался. Повторно открываю сайт. Осталось попыток {max_tries}')
        raise WebsiteError('Попытки загрузить данные исчерпаны.')

    def load_price_service(self, max_tries=5):
        """
        Обработка запроса на индексы цен товаров и услуг
        """
        while max_tries > 0:
            max_tries -= 1
            response = requests.get(cfg.url_third)
            if response.status_code != 200:
                if max_tries == 1:
                    logging.error(
                        f'Сайт {cfg.url_third} не доступен. Осталась одна попытка. Попробую открыть повторно через 1 час.')
                    self.web.wait(21, 'hours', self.queue_response, self.task_id)
                else:
                    logging.error(f'Сайт {cfg.url_third} не доступен. Пробую открыть повторно через 5 сек')
                    time.sleep(5)
            else:
                logging.info(f'Сайт {cfg.url_third} доступен. Начинаю поиск ссылки')
                try:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    for div in soup.find_all('div', attrs={'class': 'document-list__item-desc'}):
                        if div == '\n':
                            continue
                        else:
                            for tag in div:
                                if 'Индексы потребительских цен на товары и услуги по Российской Федерации, месяцы (с 1991 г.)' in \
                                        tag.text:
                                    # спускаемся к дочерним элементам, что бы забрать дату
                                    for tag in div:
                                        if tag == '\n':
                                            continue
                                        elif tag['class'][0] == 'document-list__item-info':
                                            try:
                                                self.date_update = self.str_date(tag.text, 'rosstat')
                                                break
                                            except TypeError as err:
                                                logging.error(f'Ошибка при извлечении даты выгрузки данных на сайте. {err}')
                                                raise NotFoundElement(
                                                    'Ошибка при извлечении даты выгрузки данных на сайте')
                                    # поднимаемся до соседа свверху, что бы забрать ссылку для скачивания
                                    for tag in div.previous_siblings:
                                        if tag == '\n':
                                            continue
                                        elif tag['class'][0] == 'document-list__item-link':
                                            href = [element['href'] for element in tag.children if element != '\n'][0]
                                            href = f'https://rosstat.gov.ru{href}'
                                            logging.info(f'Ссылка получена')
                                            response = requests.get(href)
                                            if response.status_code != 200:
                                                logging.error(f'Ссылка не доступна для скачивания')
                                                raise WebsiteError
                                            else:
                                                loaded_file = Path(cfg.load_dir_IPC, 'ipc_service.xlsx')
                                                with open(loaded_file, "wb") as code:
                                                    code.write(response.content)
                                                    logging.info(f'Файл скачен')
                                                    return loaded_file
                    logging.error('Не найден тег на странице. Проверьте верстку')
                    raise NotFoundElement
                except Exception as err:
                    logging.error(err)
                    raise NotFoundElement
        raise WebsiteError(f'Сайт {cfg.url_third} не доступен. Попробуйте повторный запрос чуть позже')

    def xml_to_json(self, classifier:str, period:str, load_dir, processed_folder):
        """
        Создание json файла
        :param path_to_file:путь до файла
        :param classifier:'PPI_by_OKPD' or 'PPI_by_OKPD'
        :param period:период формата '2022-03-02T15:57:25'
        :return:путь до созданного json файла
        """
        name_classifier = 's_OKPD2' if classifier == "PPI_by_OKPD" else 's_OKVED2'
        path_to_file = Path(load_dir,'57608.xml') if classifier == "PPI_by_OKPD" else Path(load_dir,'57609.xml')

        with open(path_to_file, 'r', encoding='UTF-8') as fd:
            xml_file = fd.read()

        with open(cfg.template_file_data, encoding='UTF-8') as file:
            file_data = json.load(file)
        lg.info(f'Прочитал {path_to_file}')
        soup = BeautifulSoup(xml_file, 'lxml')
        for tag in soup.findAll("generic:series"):
            if tag == '\n':
                continue
            else:
                if self.validation(tag, name_classifier, period=period):
                    dataset = self.execute_attr(tag, name_classifier)
                    file_data["data"].append(dataset)
        json_file = os.path.join(load_dir, 'file_data.json')
        #сохраняем file_data.json как json
        json.dump(file_data, open(json_file, mode='w', encoding='utf-8'), indent=4, ensure_ascii=False, default=str)
        #архивируем файлы file_data.json.json и загруженный с сайта
        self.file_compression(load_dir, processed_folder)
        self.date_update = self.str_date(soup.prepared.text, 'fedstat')
        response_path = self.create_response(load_dir, classifier, cfg.template_response)
        return response_path

    def create_response(self, load_dir, classifier, response_path):
        logging.info('Создаю response.json')
        with open(response_path, encoding='UTF-8') as file:
            response = json.load(file)
        response["header"]["subject"] = classifier
        response["header"]["timestamp"] = datetime.timestamp(datetime.now())
        response["header"]["requestID"] = self.task_id
        response["body"]["date_update"] = self.date_update
        for file in os.listdir(load_dir):
            if 'zip' in file:
                file_path = os.path.join(load_dir, file)
                if 'file_data' in file:
                    response["body"]["file_data"] = self.convert_to_base64(file_path)
                else:
                    response["body"]["files"].append({"name": file, "base64": self.convert_to_base64(file_path)})
                os.remove(os.path.join(load_dir, file))
        response_path = os.path.join(load_dir, 'response.json')
        json.dump(response, open(response_path, mode='w', encoding='utf-8'), indent=4, ensure_ascii=False, default=str)
        logging.info(f'Создан {response_path}')
        return response_path

    def convert_to_base64(self, file_path):
        """
        Кодирование файлов в base64
        :param file_path: путь до кодируемого файла
        :return:закодированный файл
        """
        with open(file_path, 'rb') as f:
            doc64 = base64.b64encode(f.read())
            logging.info(f'Закодировал {file_path} в base64')
            doc_str = doc64.decode('utf-8')
            return doc_str

    def str_date(self, date: str, name_website):
        """
        Преобразование из даты в формате строки в формат дата.
        :param period: дата в формате строки. Для сайта rosstat вх формат: 13.08.2009 Для сайта fedstat "2022-11-10"
        :return: дата в формате datetime
        """
        logging.info('Готовлю дату для self.date_update')
        new_date = ''
        if re.search(r'\d{4}-\d{2}-\d{2}|\d{2}.\d{2}.\d{4}', date):
            exctract_date = re.search(r'\d{4}-\d{2}-\d{2}|\d{2}.\d{2}.\d{4}', date)[0]
            if name_website == 'rosstat':
                new_date = datetime.strptime(exctract_date, '%d.%m.%Y')
            elif name_website == 'fedstat':
                new_date = datetime.strptime(exctract_date, '%Y-%m-%d')
            logging.info(f'self.date_update == {new_date}')
            return new_date
        else:
            logging.error('Изменился формат даты в xml файле в теге <prepared>')
            raise FileProcessError('Ошибка обработки файла. Проверьте формат даты в загруженнои файле')

    def validation(self, tag:str, classifier:str, period:str):
        """Проверка тегов внутри тега <generic:Series> на соотвествие условиям
        period == m_yyyy or None
        :param tag:теги вида <generic:SeriesKey>, <generic:Attributes>, <generic:Obs>
        :param classifer:'s_OKPD2' or 's_OKVED2'
        :param period:период формата '2022-03-02T15:57:25'
        :return:True or False
        """

        #Проверяем тег series:key
        if self.check_series_key(tag, classifier):
            if period is None:
                return True
        else:
            return False
        # Проверяем период
        if self.check_period(tag, period):
            return True
        else:
            return False

    def check_period(self, tag, period):
        """
        Поиск нужного периода в теге
        :param tag:теги вида <generic:SeriesKey>, <generic:Attributes>, <generic:Obs>
        :param period:период формата '2022-03-02T15:57:25'
        :return:True or False
        """
        # Извлекаем месяц и год из дата формата 2022-03-02T15:57:25
        year = re.findall('^\d{4}', period)[0]
        month = re.findall('-(\d{2})-', period)[0]
        found_year = None
        found_month = None

        # перебираем теги: <generic:SeriesKey>, <generic:Attributes>, <generic:Obs>
        for sub_tag in tag:
            if sub_tag == '\n':
                continue
            elif sub_tag.name == 'generic:attributes':
                # перебираем теги: <generic:Value>
                for sub_sub_tag in sub_tag:
                    if sub_sub_tag == '\n':
                        continue
                    elif sub_sub_tag.attrs.get('concept') == "PERIOD":
                        found_month = cfg.dict_months[sub_sub_tag.attrs.get('value').strip()]
                        break
            elif sub_tag.name == 'generic:obs':
                for sub_sub_tag in sub_tag:
                    if sub_sub_tag == '\n':
                        continue
                    elif sub_sub_tag.name == "generic:time":
                        found_year = sub_sub_tag.text.strip()
                        break
        if found_year and found_month:
            if found_year < year:
                return False
            elif found_year > year:
                return True
            elif found_year == year:
                if found_month >= month:
                    return True
                return False
        return False

    def execute_attr(sel, tag, classifier):
        """Извлечениеи данных из xml
        :param classifier:s_OKVED2" or s_OKPD2
        :param tag:теги вида <generic:SeriesKey>, <generic:Attributes>, <generic:Obs>
        :return:словарь с данными
        """
        data = {}
        for sub_tag in tag:
            if sub_tag == '\n':
                continue
            for sub_sub_tag in sub_tag:
                if sub_sub_tag == '\n':
                    continue

                elif sub_sub_tag.attrs.get('concept') == "PERIOD":
                    data['PERIOD'] = sub_sub_tag.attrs.get('value')

                elif sub_sub_tag.name == "generic:time":
                    data['time'] = sub_sub_tag.text

                elif sub_sub_tag.name == "generic:obsvalue":
                    data['value'] = sub_sub_tag.attrs.get('value')

                elif sub_sub_tag.attrs.get('concept') == classifier:
                    data[classifier] = sub_sub_tag.attrs.get('value')
        return data

    def check_series_key(self, tag, classifier):
        """Проверяем теги внутри <generic:SeriesKey>
        :param classifier:s_OKVED2" or s_OKPD2
        :param tag:теги вида <generic:Series>
        :return:True or False
        """
        #перебираем <generic:Value>
        found_classifier = False
        for sub_tag in tag:
            if sub_tag == '\n':
                continue
            elif sub_tag.name == 'generic:serieskey':
                for sub_sub_tag in sub_tag:
                    if sub_sub_tag == '\n':
                        continue
                    elif sub_sub_tag.attrs.get('concept') == "s_kanalreal":
                        if sub_sub_tag.attrs.get('value') != "20":
                            lg.error(f'Не найден {found_classifier}')
                            return False
                    elif sub_sub_tag.attrs.get('concept') == "s_OKATO":
                        if sub_sub_tag.attrs.get('value') != "643":
                            return False
                    elif sub_sub_tag.attrs.get('concept') == "s_POK":
                        if sub_sub_tag.attrs.get('value') != "44":
                            return False
                    elif sub_sub_tag.attrs.get('concept') == classifier:
                        found_classifier = True
                if found_classifier:
                    return True
                lg.error(f'Не найден {classifier}')
                return False

    def file_compression(self, folder_load, processed_folder):
        os.chdir(folder_load)
        for file in os.listdir(folder_load):
            if os.path.isfile(file):
                if '.xml' in file:
                    file_name = file.replace('.xml', '')
                elif '.json' in file:
                    file_name = file.replace('.json', '')
                elif '.xlsx' in file:
                    file_name = file.replace('.xlsx', '')
                else:
                    raise FileProcessError('Ошибка обработки файла. Проверьте расширение файлов')
                with zipfile.ZipFile(f'{file_name}.zip', 'w') as zip:
                    zip.write(file, compress_type=zipfile.ZIP_DEFLATED)
                logging.info(f'Архив {file_name}.zip создан')
                logging.info(f'Перемещаем "{file}" из каталога: '
                             f"{Path(folder_load, file)} в каталог: {processed_folder}")
                shutil.move(file, os.path.join(processed_folder, file))
        os.chdir(cfg.root_dir)
