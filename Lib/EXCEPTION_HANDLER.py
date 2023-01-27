import os.path
from datetime import datetime
from config import Config as cfg
from Lib.RABBIT import Rabbit
import logging
import traceback
from openpyxl.styles.borders import Border, Side
import openpyxl
import smtplib
import json




def exception_decorator(method):
    def wraper(self, *args):
        try:
           return method(self, *args)
        except WebsiteError as err:
            err.exception_handler(queue=self.queue_response,
                                  task_id=self.task_id,
                                  type_error='website_error',
                                  to_rabbit='on',
                                  to_mail='on',
                                  parameters_request=self.classifier
                                  )

        except NotFoundElement as err:
            err.exception_handler(queue=self.queue_response,
                                  task_id=self.task_id,
                                  type_error='not_found_element',
                                  to_rabbit='on',
                                  to_mail='on',
                                  parameters_request=self.classifier
                                  )
        except FileProcessError as err:
            err.exception_handler(queue=self.queue_response,
                                  task_id=self.task_id,
                                  type_error='processing_file_error',
                                  to_rabbit='on',
                                  to_mail='on',
                                  parameters_request=self.classifier
                                  )
        except FileNotFound as err:
            err.exception_handler(queue=self.queue_response,
                                  task_id=self.task_id,
                                  type_error='file_not_found',
                                  to_rabbit='on',
                                  to_mail='on',
                                  parameters_request=self.classifier
                                  )
        except Exception as err:
            logging.error(err)
            if method.__name__ == 'task_processing':
                ExceptionHandler().exception_handler(queue=self.queue_response,
                                                     task_id=self.task_id,
                                                     type_error='unknown_error',
                                                     to_rabbit='on',
                                                     to_mail='on',
                                                     parameters_request=self.classifier
                                                     )
            elif method.__name__ == 'request_validator':
                queue = self.queue_response
                if queue is None:
                    queue = cfg.queue_error
                task_id = self.task_id
                if self.task_id is None:
                    task_id = ''
                ExceptionHandler().exception_handler(queue=queue,
                                                     text_error='Ошибка в запросе',
                                                     task_id=task_id,
                                                     type_error='bad_request',
                                                     to_rabbit='on',
                                                     parameters_request=self.classifier
                                                     )

            elif method.__name__ == 'run':
                queue = self.queue_response
                if queue is None:
                    queue = cfg.queue_error
                task_id = self.task_id
                if self.task_id is None:
                    task_id = ''
                logging.error(f'Непредвиденная ошибка {err}')
                ExceptionHandler().exception_handler(queue=queue,
                                                     task_id=task_id,
                                                     type_error='unknown_error',
                                                     to_rabbit='on',
                                                     to_mail='on',
                                                     parameters_request=self.classifier,
                                                     stop_robot=True
                                                     )

        except json.JSONDecodeError as err:
            logging.error(err)
            if method.__name__ == 'request_validator':
                logging.error(f'Ошибка кодировки в запросе {err}. Ожидал json.')
                ExceptionHandler().exception_handler(queue=cfg.queue_error,
                                                     text_error='Проверьте кодировку. Ожидал json',
                                                     type_error='bad_request',
                                                     to_rabbit='on'
                                                     )

    return wraper


class ExceptionHandler:

    def exception_handler(self, queue=None, text_error='', task_id=None, type_error=None, to_rabbit='off',
                          to_mail='off', parameters_request=None, stop_robot=False):
        """
        Обработка исключений
        :param queue: очередь для отправки сообщений
        :param text_error: текст сообщения об исклчюении
        :param task_id: id запроса
        :param parameters_request: параметры запроса
        :param type_error: тип ошибки
        :param to_rabbit: отправка сообщения через rabbit
        :param to_mail: отправка сообщения через Outlook
        :param stop_robot: остановка робота
        """

        trace = traceback.format_exc()
        logging.error(f'\n\n{trace}')
        text = self.get_message(type_error=type_error, text_error=text_error, task_id=task_id)
        if to_rabbit == 'on':
            json_data = self.create_error_json(type_error=type_error, text_error=text, task_id=task_id)
            Rabbit().send_data_queue(queue_response=queue,
                                         data=json_data, type='Error')

        self.add_data_to_log(task_id, parameters_request, status=type_error)
        if to_mail == 'on':
            subject = cfg.robot_name
            body = 'Добрый день! \n ' \
                       f'{text}\n' \
                       f'{trace}'
            self.send_smtp(from_mail=cfg.robot_mail, to=cfg.support_email, subject=subject, text=body)
        if stop_robot:
            # завершаем работу робота
            exit(-1)

    @staticmethod
    def send_smtp(from_mail, to, subject, text):
        """ Отправка писем через протокол SMTP """
        logging.info('Отправка письма через протокол SMTP')
        try:
            conn = smtplib.SMTP(cfg.server_mail, 25)
            text = "From:{0}\nTo:{1}\nSubject:{2}\n\n{3}".format(
                from_mail, to, subject, text).encode("utf-8")
            conn.sendmail(from_mail, to, text)
            conn.quit()
            logging.info('Письмо отправлено')
        except Exception as err:
            logging.error(f'Ошибка при отправке письма по SMTP: {err}')

    @staticmethod
    def create_error_json(type_error, text_error, task_id):
        """
        Создание файла json о возникшем исключении
        :param type_error: тип исключения
        :param task_id: id запроса
        :param text_error: текст сообщения об исключении
        :return: json_path
        """
        with open(cfg.template_response, encoding='UTF-8') as out_file:
            logging.info(f'Записываю в json ошибку {type_error}')
            data = json.load(out_file)
            data["header"]["requestID"] = task_id
            data["header"]["timestamp"] = datetime.timestamp(datetime.now())
            data["ErrorText"] = text_error
        json_path = os.path.join(cfg.send_error_dir, f'error_message_{datetime.today().strftime("%d.%m.%Y %H-%M")}.json')
        with open(json_path, mode='w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        logging.info(f'Запись ошибки {type_error}: в JSON закончена')
        return json_path

    @staticmethod
    def add_data_to_log(task, parameters_request,  status):
        """
        Записываем в log.xlsx информацию полученных запросах
        :param task: id запроса
        :param forecast: прогноз
        :param status: статус выполненого задания
        """
        BEGIN_ROW_PATTERN = 10
        BORDER = Border(left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin'))
        data = {
            1: datetime.today().strftime("%d.%m.%Y %H:%M"),
            2: task,
            3: status,
            4: parameters_request
        }
        logging.info(f'Открываем файл: log.xlsx')
        workbook = openpyxl.load_workbook(cfg.log_xlsx)
        logging.debug('workbook.active')
        worksheet = workbook.active
        end_row = worksheet.max_row
        logging.debug('ws_pattern_total.max_row')
        row = 1
        while True:
            if worksheet.cell(row=row, column=1).value is None:
                break
            row += 1
        for number_column, value in data.items():
            logging.info(f'Вносим значение: {value}')
            worksheet.cell(row=row, column=number_column).value = value
            worksheet.cell(row=row, column=number_column).border = BORDER
        workbook.save(cfg.log_xlsx)
        logging.info('Запись в логфайл добавлена')

    @staticmethod
    def get_message(type_error, *, text_error=None, task_id=''):
        """
        Формируем сообщение об ошибки
        :param task_id: id запроса
        :param type_error: тип исключения
        :param text_error: текст сообщения об исключении
        :return: текст сообщения об исключении
        """
        text = ''
        if type_error == 'processing_file_error':
            text = f'Ошибка обработки файла. Запрос {task_id} завершился неудачно'
        elif type_error == 'download_error':
            text = f'Ошибка при загрузке данных c сайта. Запрос {task_id} завершился неудачно'
        elif type_error == 'website_error':
            text = f'Нет доступа к сайту. Запрос {task_id} завершился неудачно.'
        elif type_error == 'unknown_error':
            text = f'Непредвиденная ошибка при выполнении запроса {task_id}.'
        elif type_error == 'connect_rabbit_error':
            text = 'Не удалось подключиться к серверу RabbitMQ.'
        elif type_error == 'bad_request':
            text = f'Ошибка обработки запроса {task_id}. {text_error}'
        elif type_error == 'no_updates':
            text = f'На сайте обновлений не обнаружено.'
        elif type_error == 'not_found_element':
            text = f'Не удалось найти элемент на странице. Проверьте верстку на сайте. Запрос {task_id} завершился неудачно'
        elif type_error == 'not_found_data':
            text = f'Данные в запрашиваемом периоде не найдены. Повторите запрос. Запрос {task_id} завершился неудачно'
        elif type_error == 'exctract_error':
            text = f'Не найден файл в архиве. Запрос {task_id} завершился неудачно'
        elif type_error == 'robot_sleep':
            text = f'Сайт не доступен. Повторную попытку робот осуществит в 21-00'
        elif type_error == 'file_not_found':
            text = f'Робот не нашел загруженный с сайта файл'
        return text


class WebsiteError(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class DownloadError(Exception,  ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class BadPeriodError(Exception,  ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class FileProcessError(Exception,  ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class NoUpdatesError(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class BadRequest(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text


class NotFoundElement(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text

class ExctractError(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text

class FileNotFound(Exception, ExceptionHandler):
    def __init__(self, text):
        self.txt = text