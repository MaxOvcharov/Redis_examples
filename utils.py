# -*- coding: utf-8 -*-
import json
import yaml
import uuid

from datetime import datetime, date


class CustomJsonEncoder(json.JSONEncoder):
    """
    Пользовательский класс, предназначенный для кодирования
      UUID и даты в формат необходимый для корректной работы
      json модуля.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def load_config(file_name):
    """
    Данный метод предназначен для загрузки конфигурационного
      файла в формате *.yaml.

    :param str file_name: абсолютный путь к конфигурационому файлу.

    :return config: ассоциативный массив с конфигурационными параметрами,
      разделенных по секциям.
    :rtype: dict
    """
    with open(file_name, 'rt') as f:
        try:
            config = yaml.load(f)
        except yaml.error.MarkedYAMLError as e:
            raise RuntimeError('Incorrect configuration yaml file format: %s' % e)
        return config


def serialize_json(data, **options):
    """
    Данный метод предназначен для конвертирования
      Python объекта в JSON

    :param dict data: данные для преобразования в JSON
    :param options: дополнительные опции для конвертирования

    :return: JSON объект
    :rtype: json.encoder.JSONEncoder
    """
    return json.dumps(data, cls=CustomJsonEncoder, **options)


def deserialize_json(data, **options):
    """
    Данный метод предназначен для конвертирования
      из JSON объекта в Python объект.

    :param data: данные для преобразования в JSON
    :type data: json.encoder.JSONEncoder
    :param options: дополнительные опции для конвертирования

    :return: Python объект
    :rtype: dict
    """
    return json.loads(data, **options)
