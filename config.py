import os
from pathlib import Path

MODE = 'Test'


class Config:
    # общие параметры для тестовой и продуктовой среды
    root_dir = os.path.dirname(os.path.abspath(__file__))
    mode = MODE
    robot_name = 'Робот: Получение информации по индексации цен из Росстата/ЕМИСС'
    url_first = 'https://fedstat.ru/indicator/57609'  # ссылка сайта
    url_second = 'https://fedstat.ru/indicator/57608'  # ссылка сайта
    url_third = 'https://rosstat.gov.ru/statistics/price#'  # ссылка сайта
    xpath_download = {'downfile': {'select': "//button[text()='Скачать']", 'Load': "//*[@id='download_sdmx_file']"}}
    send_error_dir = os.path.join(root_dir, 'send_error') # путь до каталога с отправленными сообщениями об ошибках в rabbit
    logs_folder_path = os.path.join(root_dir, 'Logs')
    log_file = "robot.log"
    log_xlsx = os.path.join(logs_folder_path, 'log.xlsx')
    template_dir = os.path.join(root_dir, 'Templates')
    load_dir = os.path.join(root_dir, 'Load')  # путь до каталога загрузки# параметры отличающиеся для тестовой среды
    load_dir_OKPD = os.path.join(load_dir, 'OKPD')  # путь до каталога загрузки ОКПД
    load_dir_OKVED = os.path.join(load_dir, 'OKVED')  # путь до каталога загрузки ОКВЭД
    load_dir_IPC = os.path.join(load_dir, 'IPC')  # путь до каталога загрузки ИПЦ
    processed_files_OKPD = os.path.join(load_dir_OKPD, 'Processed_files') # путь до отправленных файлов в rabbit
    processed_files_OKVED = os.path.join(load_dir_OKVED, 'Processed_files') # путь до отправленных файлов в rabbit
    processed_files_IPC = os.path.join(load_dir_IPC, 'Processed_files') # путь до отправленных файлов в rabbit
    template_response = os.path.join(template_dir, 'response.json') #шаблон ответа в rabbit
    template_file_data = os.path.join(template_dir, 'file_data.json') #шаблон для записи данных из скаченных файлов
    robot_mail = ''
    # НАстройка почты
    server_mail = ""

    dict_months = {'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04', 'май': '05', 'июнь': '06',
                     'июль': '07', 'август': '08', 'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'}

    if mode == 'Test':
        xpath_url_first = {

            'okato': {
                'select': "//a[text()='Классификатор объектов административно-территориального деления (ОКАТО)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Российская Федерация']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'home_market': {
                'select': "//div[@class='k-grid-cover-title']/a[text()='Внутренний рынок']//following-sibling::span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Внутренний рынок']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'indicator_types': {
                'select': "//a[text()='Виды показателя']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='К предыдущему месяцу']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'year': {'select': "//a[@class='k-grid-filter mrl22m k-state-active k-fil-non']//span",
                     'clean': "//*[@id='grid']//button[text()='Очистить']",
                     'mark': "//input[@value='2022']/following-sibling::span",
                     'filter': "//*[@id='grid']//button[text()='Фильтровать']"
                     },

            'okved': {
                'select': "//a[text()='Классификатор видов экономической деятельности (ОКВЭД2)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//b[contains(text(), 'Выбрать все')]",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
        }

        xpath_url_second = {

            'chanel': {
                'select': "//a[text()='Каналы реализации']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Внутренний рынок']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'indicator_types': {
                'select': "//a[text()='Виды показателя']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='К предыдущему месяцу']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'okato': {
                'select': "//a[text()='Классификатор объектов административно-территориального деления (ОКАТО)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Российская Федерация']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'okpd': {
                'select': "//a[text()='Классификатор продукции по видам экономической деятельности (ОКПД2)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//b[contains(text(), 'Выбрать все')]",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'year': {
                'select': "//a[@class='k-grid-filter mrl22m k-state-active k-fil-non']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//input[@value='2022']/following-sibling::span",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

        }
        driver_path = r'/usr/bin/chromedriver' # путь до драйвера
        browser_path = r'/usr/lib/chromium/chromium' # путь до браузера
        support_email = ''

        #Настройка для Rabbit
        HOST = ''
        LOGIN = ''
        PWD = os.environ['rpauser']
        queue_request = ''
        PORT = 5672  # Порт для подключения к серверу с rabbit
        PATH = '/'
        queue_error = ''

    else:
        xpath_url_first = {

            'okato': {
                'select': "//a[text()='Классификатор объектов административно-территориального деления (ОКАТО)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Российская Федерация']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'home_market': {
                'select': "//div[@class='k-grid-cover-title']/a[text()='Внутренний рынок']//following-sibling::span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Внутренний рынок']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'indicator_types': {
                'select': "//a[text()='Виды показателя']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='К предыдущему месяцу']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'year': {'select': "//a[@class='k-grid-filter mrl22m k-state-active k-fil-non']//span",
                     'clean': "//*[@id='grid']//button[text()='Очистить']",
                     'mark': "//b[contains(text(), 'Выбрать все')]",
                     'filter': "//*[@id='grid']//button[text()='Фильтровать']"
                     },

            'okved': {
                'select': "//a[text()='Классификатор видов экономической деятельности (ОКВЭД2)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//b[contains(text(), 'Выбрать все')]",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
        }

        xpath_url_second = {

            'chanel': {
                'select': "//a[text()='Каналы реализации']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Внутренний рынок']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'indicator_types': {
                'select': "//a[text()='Виды показателя']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='К предыдущему месяцу']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },
            'okato': {
                'select': "//a[text()='Классификатор объектов административно-территориального деления (ОКАТО)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//label[text()='Российская Федерация']//span[@class='sp_checkbox']",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'okpd': {
                'select': "//a[text()='Классификатор продукции по видам экономической деятельности (ОКПД2)']/preceding-sibling::a[@class='k-grid-filter k-state-active']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//b[contains(text(), 'Выбрать все')]",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

            'year': {
                'select': "//a[@class='k-grid-filter mrl22m k-state-active k-fil-non']//span",
                'clean': "//*[@id='grid']//button[text()='Очистить']",
                'mark': "//b[contains(text(), 'Выбрать все')]",
                'filter': "//*[@id='grid']//button[text()='Фильтровать']"
            },

        }

    # Создаем директории
    [os.makedirs(dir, exist_ok=True) for dir in
     [
         logs_folder_path,
         send_error_dir,
         template_dir,
         load_dir,
         load_dir_OKPD,
         load_dir_OKVED,
         load_dir_IPC,
         processed_files_OKPD,
         processed_files_OKVED,
         processed_files_IPC
     ]]










