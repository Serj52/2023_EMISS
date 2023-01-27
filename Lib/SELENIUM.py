import logging

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import os
import logging as lg
from datetime import datetime
import time
from config import Config as cfg
from Lib.EXCEPTION_HANDLER import WebsiteError, DownloadError, NotFoundElement
from Lib import EXCEPTION_HANDLER


class Connection:
    """
    рыба для селениума
    """

    def __init__(self):
        self.config = cfg()
        self.options = webdriver.ChromeOptions()
        self.driver = None
        self.browser_path = cfg.browser_path
        self.driver_path = cfg.driver_path

    def set_options(self, load_dir):
        self.options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.options.binary_location = self.browser_path
        self.options.add_experimental_option('prefs', {'download.default_directory': load_dir,
                                                       "safebrowsing.enabled": True,
                                                       "download.prompt_for_download": False,
                                                       "download.directory_upgrade": True,
                                                       })
        # self.options.add_argument("--headless")

    def set_driver(self):
        self.driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)

class Selenium:
    """Класс по работе с Selenium"""
    def __init__(self):
        self.conn = Connection()
        self.timeout = 600

    def open_site(self, site_url, queue_response, task_id, load_dir, max_tries=5):
        while max_tries > 0:
            try:
                self.conn.set_options(load_dir)
                self.conn.set_driver()
                lg.info(f'Open URL:{site_url}')
                self.conn.driver.set_page_load_timeout(self.timeout)
                max_tries -= 1
                self.conn.driver.get(site_url)
                self.conn.driver.maximize_window()
                return
            except TimeoutException:
                if max_tries == 1:
                    lg.error(f'Сайт не открылся')
                    self.close_site()
                    self.wait(21, 'h', queue_response, task_id)
                elif max_tries != 0:
                    time.sleep(5)
                    self.close_site()
                    lg.error(f'Сайт не открылся. Повторно открываю сайт. Осталось попыток {max_tries}')
        lg.error(f'Попытки открыть сайт исчерапны')
        self.close_site()
        raise WebsiteError('Нестабильная работа сайта')

    def wait(self, value, type, queue_response, task_id):
        lg.info(f'Засыпаю до {value} {type}')
        EXCEPTION_HANDLER.ExceptionHandler().exception_handler(queue=queue_response,
                                                               task_id=task_id,
                                                               type_error='robot_sleep',
                                                               to_rabbit='on',
                                                               )
        if type in ['hours', 'h', 'H', 'hour']:
            while True:
                now = datetime.now()
                hours = now.hour
                if hours == value:
                    lg.info(f'Проснулся')
                    return
                else:
                    time.sleep(60)
                    continue
        elif type in ['seconds', 'second', 's']:
            time.sleep(value)
            lg.info(f'Проснулся')
            return
        else:
            lg.error('Указан неправильный формат type')
            raise

    def close_site(self):
        try:
            self.conn.driver.implicitly_wait(2)
            lg.info("Идет завершение сессии...")
            self.conn.driver.quit()
            lg.info("Драйвер успешно завершил работу.")
        except Exception as err:
            # возможно стоит выводить тип ошибки трассировку
            lg.info(f"Произошла ошибка при завершении работы с браузером: '{err}'")
            os.system("pkill chromium")
            lg.info('Chrome браузер закрыт принудительно.')
            os.system("pkill chromedriver")
            lg.info('Chrome драйвер закрыт принудительно.')

    def find_by_xpath(self, selector):
        """Поиск вебэлемента по xpath"""
        while True:
            try:
                wt = WebDriverWait(self.conn.driver, timeout=self.timeout)
                # Ожидание загрузки страницы
                wt.until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='agrid-loader']")))
                return self.conn.driver.find_element(By.XPATH, selector)
            except TimeoutException as err:
                lg.error(f'Время ожидания загрузки элемента истекло {err}')
                raise TimeoutException
            except Exception as err:
                raise NotFoundElement(f'Не найден селектор {selector}. Ошибка {err}')

    def download_file(self, max_tries=2):
        """в этом блоке скачивается файл"""
        xpath = cfg.xpath_download
        while True:
            try:
                wt = WebDriverWait(self.conn.driver, self.timeout)
                logging.info('Ожидаю прогрузки последнего фильтра')
                wt.until(EC.element_to_be_clickable((By.XPATH, xpath['downfile']['select'])))
                logging.info('Начинаю загрузку данных')
                self.conn.driver.find_element(By.XPATH, xpath['downfile']['select']).click()
                wt.until(EC.element_to_be_clickable((By.XPATH, xpath['downfile']['Load'])))
                logging.info('Выбираю загрузку файла в формате xml')
                self.conn.driver.find_element(By.XPATH, xpath['downfile']['Load']).click()
                lg.info(f'Данные загружены')
                return
            except TimeoutException as err:
                if max_tries == 0:
                    lg.error(err)
                    raise TimeoutException
                lg.info(f'Пробуем еще раз загрузить данные. Осталось попыток {max_tries}')
                time.sleep(2)
                max_tries -= 1
            except Exception as err:
                lg.error(f'Данные не загружены {err}')
                if max_tries == 0:
                    lg.error('Количество попыток загрузить данные исчерпано')
                    raise DownloadError('Не удалось загрузить данные с сайта')
                lg.info('Пробуем еще раз загрузить данные')
                time.sleep(2)
                max_tries -= 1

    def set_filter(self, xpath:dict):
        """
        Фильтрация данных
        :param xpath:словарь с набором селекторов
        """
        lg.info(f'Фильтрация данных началась')
        for filters in xpath:
            lg.info(f"Выставляю фильтр {filters}")
            for filter, selector in xpath[filters].items():
                lg.info(f"Нажимаю {filter}")
                self.find_by_xpath(selector).click()
            lg.info(f"Фильтр {filters} выставлен")
            # Ожидание прогрузки фильтрации
            lg.info('Данные отфильтрованы')

    def refresh_file(self, name, load_dir, max_tries=5):
        """
        в этом блоке перезаписывается загруженный файл
        :param name:имя файла
        :return:путь до нового файла
        """
        url_name = f'{name}.xml'
        path_old_file = os.path.join(load_dir, 'data.xml')
        path_new_file = os.path.join(load_dir, url_name)
        while max_tries >= 0:
            try:
                time.sleep(3)
                os.rename(path_old_file, path_new_file)
                return path_new_file
            #Если файл с таким именем существует, то его удаляем, новый переименуем
            except FileExistsError:
                os.remove(path_new_file)
                os.rename(path_old_file, path_new_file)
                lg.info(f'Файл {url_name} перезаписан')
                return path_new_file
            except FileNotFoundError:
                lg.info(f'Файл не найден. Пробую еще раз найти')
                time.sleep(10)
                max_tries -= 1
        lg.error(f'Количество попыток исчерпано. Файла "data" нет в директории')
        raise EXCEPTION_HANDLER.FileNotFound('Скаченный файл не найден в директории')
