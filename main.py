from business import Business
from config import Config as cfg
import logging as lg
from Lib import log
import pika
import logging
import json
from Lib import EXCEPTION_HANDLER
import re
from pika.exceptions import AMQPConnectionError
import time
import os


class Robot(Business):
    """
    Название:
    Аналитик:
    Разработчик: Цыганов С.А.
    """

    def __init__(self):
        super().__init__()

    @EXCEPTION_HANDLER.exception_decorator
    def run(self, max_tries=5):
        """
        Запуск робота
        :param max_tries: число попыток подключиться к RabbitMq
        """
        while 0 >= max_tries or max_tries <= 5:
            try:
                self.queue_processing()
            except AMQPConnectionError:
                logging.error('Ошибка подключения к серверу RabbitMQ')
                if max_tries == 0:
                    EXCEPTION_HANDLER.ExceptionHandler().exception_handler(
                        type_error='connect_rabbit_error',
                        to_mail='on',
                        stop_robot=True
                    )
                    break
                else:
                    max_tries -= 1
                    logging.error('Пробую повторно подключиться к серверу RabbitMQ через 1мин')
                    time.sleep(60)
                    continue

    @EXCEPTION_HANDLER.exception_decorator
    def task_processing(self):
        processed_folder = cfg.processed_files_OKVED if 'OKVED' in self.classifier else cfg.processed_files_OKPD
        if self.classifier == "PPI_by_OKVED":
            self.fedstat_work(url=cfg.url_first, xpath=cfg.xpath_url_first, name_file=cfg.url_first[-5:],
                              classifer=self.classifier, period=self.period, load_dir=cfg.load_dir_OKVED,
                              processed_folder=processed_folder)
        elif self.classifier == "PPI_by_OKPD":
            self.fedstat_work(url=cfg.url_second, xpath=cfg.xpath_url_second, name_file=cfg.url_second[-5:],
                              classifer=self.classifier, period=self.period, load_dir=cfg.load_dir_OKPD,
                              processed_folder=processed_folder)
        elif self.classifier == "IPC_service_EMISS":
            self.rosstat_work(period=self.period, classifer=self.classifier)

    def queue_processing(self):
        """
        Проверка очереди в бесконечном цикле
        """
        credentials = pika.PlainCredentials(cfg.LOGIN, cfg.PWD)
        parameters = pika.ConnectionParameters(host=cfg.HOST, port=cfg.PORT, virtual_host=cfg.PATH,
                                               credentials=credentials, heartbeat=0)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        # Создается очередь.устойчивая очередь к падению сервера с rabbit mq
        # channel.queue_declare(queue=cfg.queue_request, durable=True)
        logging.info('Подключение к RabbitMQ прошло успешно')

        def callback(ch, method, properties, body):
            logging.info(f'Получен запрос')
            self.type_request = None
            self.queue_response = None
            self.task_id = None
            self.classifier = None
            self.period = None
            # не давать нов задачу пока не сделает имеющуюся
            ch.basic_qos(prefetch_count=1)
            # Подтверждение получения сообщения. Без него сообщения будут выводиться заново после падения обработчика.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if self.request_validator(body) is True:
                self.task_processing()
                logging.info('Жду запрос')
        try:
            logging.info('Жду запрос')
            channel.basic_consume(queue=cfg.queue_request, on_message_callback=callback)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            logging.info("Обработка очереди завершена")
            channel.close()
            logging.info("Соединение закрыто")

    @EXCEPTION_HANDLER.exception_decorator
    def request_validator(self, body):
        """
        Проверка влидности запроса
        :param data: десериализованное тело запроса
        :return: True or False
        """
        # try:
        data = json.loads(body)
        logging.info(data)
        errors = None

        if data['header']['replayRoutingKey'] == '':
            errors = 'Поле replayRoutingKey в запросе пустое. '
        else:
            self.queue_response = data['header']['replayRoutingKey']
        if data['header']['subject'] == '':
            errors = 'Поле subject в запросе пустое. '
        else:
            if data['header']['subject'] == 'PPI_by_OKVED' or data['header']['subject'] == 'PPI_by_OKPD' or \
                    data['header']['subject'] == 'IPC_service_EMISS':
                self.classifier = data['header']['subject']
            else:
                errors = f"В запросе subject должен быть 'PPI_by_OKVED' или PPI_by_OKPD или IPC_service_EMISS. Получили {data['header']['subject']}"

        if data['header']["requestID"] == '':
            errors = 'Поле requestID в запросе пустое. '
        else:
            self.task_id = data['header']["requestID"]

        if data['body']["DateBegin"] != '':
            if re.search('\d\d\d\d-\d\d-\d\d', data['body']["DateBegin"]):
                self.period = data['body']["DateBegin"]
            else:
                errors = 'Проверьте формат даты в DateBegin. '
        elif data['body']["DateBegin"] == '':
            self.period = None
        if errors:
            logging.error(errors)
            queue = data['header']['replayRoutingKey']
            if data['header']['replayRoutingKey'] == '':
                queue = cfg.queue_error
            EXCEPTION_HANDLER.ExceptionHandler().exception_handler(queue=queue,
                                                                   text_error=errors,
                                                                   type_error='bad_request',
                                                                   to_rabbit='on'
                                                                   )
            logging.info(f'Данные не валидны')
            return False
        logging.info(f'Данные валидны')
        return True



if __name__ == '__main__':
    log.set_2(cfg)
    logging.getLogger("pika").setLevel(logging.WARNING)
    lg.info('\n\n=== Start ===\n\n')
    lg.info(f'Режим запуска: {cfg.mode}')
    robot = Robot()
    os.system("pkill chromium")
    robot.run()