# -*- coding: utf-8 -*-
import json
import sqlite3

import requests
from requests.exceptions import ConnectionError


def get_relevant_campaigns():
    """Возвращает актуальные компании в формате json
    Предполагаемая структура ответа:
    {
      "result": {
        "Campaigns": [{
          "Id": (long),
          "Name": (string)}
          ]
      }
    }"""

    # --- Входные данные ---
    #  Адрес сервиса Campaigns для отправки JSON-запросов (регистрозависимый)
    CampaignsURL = 'https://api.direct.yandex.com/json/v5/campaigns'
    # OAuth-токен пользователя, от имени которого будут выполняться запросы
    token = 'AgAAAAAcmsdVAANksSnYBU3Vyka1njL7fNfx1f4'
    # --- Подготовка, выполнение и обработка запроса ---
    #  Создание HTTP-заголовков запроса
    headers = {"Authorization": "Bearer " + token,  # OAuth-токен. Использование слова Bearer обязательно
               }
    # Создание тела запроса
    body = {"method": "get",  # Используемый метод.
            "params": {"SelectionCriteria": {},
                       # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                       "FieldNames": ["Id", "Name"]  # Имена параметров, которые требуется получить.
                       }}
    # Кодирование тела запроса в JSON
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
    # Информация о происходящих ошибках
    error_message = {"Error": "Произошла непредвиденная ошибка."}
    # Выполнение запроса
    try:
        result = requests.post(CampaignsURL, jsonBody, headers=headers)

        # Отладочная информация
        # print("Заголовки запроса: {}".format(result.request.headers))
        # print("Запрос: {}".format(u(result.request.body)))
        # print("Заголовки ответа: {}".format(result.headers))
        # print("Ответ: {}".format(u(result.text)))
        # print("\n")

        # Обработка запроса
        if result.status_code != 200 or result.json().get("error", False):
            error_message["Error"] = "Произошла ошибка при обращении к серверу API Директа."
            error_message["Код ошибки"] = result.json()["error"]["error_code"]
            error_message["Описание ошибки"] = result.json()["error"]["error_detail"]
            error_message["RequestId"] = result.headers.get("RequestId", False)
        else:
            # Возвращаем актуальные компании
            return result.json()

    # Обработка ошибки, если не удалось соединиться с сервером API Директа
    except ConnectionError:
        # В данном случае рекомендуется повторить запрос позднее
        error_message = {"Error": "Произошла ошибка соединения с сервером API."}

    # Если возникла какая-либо другая ошибка
    except:
        error_message = {"Error": "Произошла непредвиденная ошибка."}

    # Если функция не вернула актуальные компании, то возвращаем сообщение об ошибке
    return error_message


def insert_campaigns_to_db(campaigns_json):
    # Местоположение нашей базы данных

    db_path = './yandex_direct_sqlite3.db'
    # Статус подключения к базе данных
    sqlite_connection = False
    try:
        sqlite_connection = sqlite3.connect(db_path)
        cursor = sqlite_connection.cursor()
        # print("База данных создана и успешно подключена к SQLite")

        # Проверяем существование требуемой таблицы
        sqlite_select_query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='relevant_campaigns';"
        cursor.execute(sqlite_select_query)

        # Если количество не равно единице, то таблица не существует
        if cursor.fetchone()[0] != 1:
            # Таблица НЕ существует

            # Создаём таблицу, чтобы не загружать одинаковые кампании сделаем каждую строчку уникальной
            create_database_command = "CREATE TABLE relevant_campaigns (CampaingID int, Name varchar(255), UNIQUE(CampaingID, Name));"
            cursor.execute(create_database_command)

        # Загружаем компании в таблицу
        for campaign in campaigns_json["result"]["Campaigns"]:
            # Добавляем только уникальные строки
            insert_campaign_command = "INSERT OR IGNORE INTO relevant_campaigns (CampaingID, Name) VALUES (?, ?);"
            data_tuple = (campaign["Id"], campaign["Name"])
            cursor.execute(insert_campaign_command, data_tuple)

        sqlite_connection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    except:
        print("Произошла неизвестная ошибка при работе с базой данных")
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            # print("Соединение с SQLite закрыто")


if __name__ == "__main__":
    campaigns_json = get_relevant_campaigns()
    if "Error" not in campaigns_json.keys():
        # Если актуальные компании удалось получить, то записываем их в базу данных.
        insert_campaigns_to_db(campaigns_json)
    else:
        # Если получена ошибка, то выводим её
        print(campaigns_json)
