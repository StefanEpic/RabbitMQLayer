import copy
import json
import threading
import time
from typing import List, Dict, Any, Union, Optional

import pika
import psutil
import requests
from pika.connection import URLParameters

MAIN_IP_ADDRESS = "localhost"


class RabbitService:
    def __init__(
            self,
            service_name: str,
            service_port: int,
            rabbit_url: str,
            rabbit_workers_devices: List[str],
            subscribe_queue: str = None,
            publish_queue: str = None,
            workers_kwargs: Dict[str, Any] = None,
    ):
        """
        Абстракция сервиса обработки.
        :param service_name: Название сервиса
        :param service_port: Порт сервиса
        :param rabbit_url: Ссылка для подключения к RabbitMQ вида amqp://guest:guest@rabbitmq:5672/
        :param rabbit_workers_devices: Настройки обработчиков RabbitMQ в формате ['cpu:0:1', 'cuda:0:1']
        :param subscribe_queue: Название очереди RabbitMQ, на которую должен быть подписан сервис
        :param publish_queue: Название очереди RabbitMQ, в которую сервис должен отправлять результаты обработки
        :param workers_kwargs: Дополнительные параметры запуска сервиса
        """
        self.service_name = service_name
        self.service_port = service_port
        self.service_url = f"http://{MAIN_IP_ADDRESS}:{service_port}"
        self.rabbit_url = rabbit_url
        self.subscribe_queue = subscribe_queue
        self.publish_queue = publish_queue
        self.rabbit_workers_devices = rabbit_workers_devices
        self.workers_kwargs = workers_kwargs

    def __repr__(self) -> str:
        return self.service_name

    def up(self):
        """
        Запуск сервиса (запуск процесса обработки с использованием RabbitMQ).
        :return: None
        """
        data: Dict[str, Any] = {
            "rabbit_url": self.rabbit_url,
            "rabbit_workers_devices": self.rabbit_workers_devices,
        }
        if self.subscribe_queue:
            data["rabbit_subscribe_queue"] = self.subscribe_queue
        if self.publish_queue:
            data["rabbit_publish_queue"] = self.publish_queue
        if self.workers_kwargs:
            data["rabbit_workers_kwargs"] = self.workers_kwargs
        r = requests.post(url=f"{self.service_url}/run_rabbit_workers", data=json.dumps(data))
        if r.status_code != 200:
            print(self.service_name, r.status_code, r.json(), flush=True)

    def down(self):
        """
        Остановка сервиса (остановка процесса обработки с использованием RabbitMQ).
        :return: None
        """
        r = requests.post(url=f"{self.service_url}/stop_rabbit_workers")
        if r.status_code != 200:
            print(r.status_code, r.json(), flush=True)

    def restart(self):
        """
        Перезапуск сервиса.
        :return: None
        """
        self.down()
        self.up()


class RabbitServiceManager:
    def __init__(self, rabbit_url: str, list_of_services: List[Dict[str, Any]],
                 services_names_for_balance: List[str] = None, reserved_cpu_cores: List[int] = None,
                 time_to_rebalance: int = 3600):
        """
        Менеджер управления сервисами RabbitService.
        :param rabbit_url: Ссылка для подключения к RabbitMQ вида amqp://guest:guest@rabbitmq:5672/
        :param list_of_services: Список параметров сервисов для инициализации RabbitService вида:
            [
                {
                    'service_name': 'translation',
                    'service_port': 7012,
                    'rabbit_workers_devices': ['cpu:0:1']
                    'workers_kwargs': {'send_to_ebz': True},
                }
            ]
        WARNING: Не использовать символ "-" в создании названий контейнеров
        :param services_names_for_balance: Список названий воркеров, участвующих в балансировке (должны соответствовать service_name)
        :param reserved_cpu_cores: Список ядер CPU, на которых организована балансировка
        :param time_to_rebalance: С каким интервалом проиходит балансировка (в секундах)
        """
        self.rabbit_url = rabbit_url
        self.services: List[RabbitService] = []
        self.queues: List[str] = []
        self.queues_init: bool = False
        self._set_services(list_of_services=list_of_services)
        self.services_names_for_balance = services_names_for_balance
        self.reserved_cpu_cores = reserved_cpu_cores
        self.time_to_rebalance = time_to_rebalance
        self.first_start = None
        self.ram_control_thread: Optional[threading.Thread] = None
        self.run_ram_control_thread: bool = False
        self.balancer_thread: Optional[threading.Thread] = None
        self.run_balancer_thread: bool = False

    @staticmethod
    def _check_uncorrect_symbol(list_of_services: List[Dict[str, Any]]):
        """
        Поиск некорректных символов..
        :param list_of_services: Список параметров сервисов для инициализации RabbitService
        :return: None
        """
        for i in list_of_services:
            if "-" in i["service_name"]:
                raise "Недопустимый символ в названии контейнера"

    def _set_services(self, list_of_services: List[Dict[str, Any]]):
        """
        Генерация списка очередей и сервисов типа RabbitService.
        :param list_of_services: Список параметров сервисов для инициализации RabbitService
        :return: None
        """
        if len(list_of_services) < 2:
            raise "Для построения обработки необходимо хотя бы 2 сервиса."
        self._check_uncorrect_symbol(list_of_services)
        # Создание названий очередей
        list_of_services_names = [s["service_name"] for s in list_of_services]
        for i in range(len(list_of_services_names))[:-1]:
            self.queues.append(f"{str(i + 1).zfill(2)}_{list_of_services_names[i]}-{list_of_services_names[i + 1]}")

        # Создание классов Реббит сервисов
        for s in list_of_services:
            self.services.append(
                RabbitService(
                    service_name=s["service_name"],
                    service_port=s["service_port"],
                    rabbit_url=self.rabbit_url,
                    rabbit_workers_devices=s["rabbit_workers_devices"],
                    workers_kwargs=s.get("workers_kwargs", None),
                )
            )

        # Назначение очередей Реббит сервисам
        self.services[0].publish_queue = self.queues[0]
        self.services[-1].subscribe_queue = self.queues[-1]
        for i, s in enumerate(self.services[1:-1]):
            s.subscribe_queue = self.queues[i]
            s.publish_queue = self.queues[i + 1]

    def _create_queues(self):
        """
        Создание очередей в RabbitMQ.
        :return: None
        """
        if not self.queues_init:
            connection = pika.BlockingConnection(URLParameters(self.rabbit_url))
            channel = connection.channel()
            for q in self.queues:
                channel.queue_declare(queue=q, durable=True, arguments={"x-max-priority": 5})
            connection.close()
            self.queues_init = True

    def _remove_queues(self):
        """
        Удаление очередь из RabbitMQ с содержимым.
        :return: None
        """
        connection = pika.BlockingConnection(URLParameters(self.rabbit_url))
        channel = connection.channel()
        for q in self.queues:
            channel.queue_delete(queue=q)
        connection.close()
        self.queues_init = False

    def run_service(self, name_or_port: Union[str, int]):
        """
        Запуск одного конкретного сервиса.
        :param name_or_port: Название или порт сервиса
        :return: None
        """
        if isinstance(name_or_port, str):
            service = [s for s in self.services if s.service_name == name_or_port]
        elif isinstance(name_or_port, int):
            service = [s for s in self.services if s.service_port == name_or_port]
        else:
            return

        if service:
            service[0].up()

    def stop_service(self, name_or_port: Union[str, int]):
        """
        Остановка одного конкретного сервиса.
        :param name_or_port: Название или порт сервиса
        :return: None
        """
        if isinstance(name_or_port, str):
            service = [s for s in self.services if s.service_name == name_or_port]
        elif isinstance(name_or_port, int):
            service = [s for s in self.services if s.service_port == name_or_port]
        else:
            return

        if service:
            service[0].down()

    def run_all_services(self):
        """
        Запуск всех сервисов.
        :return: None
        """
        self._create_queues()
        self._run_ram_control()
        self._run_balance_in_thread()
        for s in self.services:
            s.up()

    def stop_all_services(self):
        """
        Остановка всех сервисов.
        :return: None
        """
        self.first_start = None
        self.ram_control_thread = None
        self.run_ram_control_thread = False
        self.balancer_thread = None
        self.run_balancer_thread = False
        for s in self.services:
            s.down()

    def restart_all_services(self):
        """
        Перезапуск всех сервисов.
        :return: None
        """
        for s in self.services:
            s.restart()

    def get_statistic(self) -> Dict[str, int]:
        """
        Получить статиску по количеству сообщений в очередях.
        :return: Словарь {Название сервиса: количество сообщений}
        """
        result = {}
        connection = pika.BlockingConnection(URLParameters(self.rabbit_url))
        channel = connection.channel()
        for q in sorted(self.queues):
            try:
                queue_info = channel.queue_declare(queue=q, passive=True)
                result[q.split('-')[-1]] = queue_info.method.message_count
            except Exception as e:
                print("Не удалось получить данные из RabbitMQ:", e, flush=True)
                pass
        connection.close()
        return result

    def _ram_control(self):
        """
        Функция контроля ОЗУ и перезагрузки сервисов в случае выхода за лимиты ОЗУ.
        :return: None
        """
        while self.run_ram_control_thread:
            current_ram = psutil.virtual_memory().percent
            if current_ram > 90:
                for s in self.services:
                    if s.workers_kwargs:
                        device = s.workers_kwargs.get("device", None)
                        if device == "cpu":
                            s.restart()
                print(f"Обнаружено привышение ОЗУ {current_ram}%/100%", flush=True)
                print(f"Выполнеена перезагрузка обработчиков, использующих CPU", flush=True)
            time.sleep(30)

    def _run_ram_control(self):
        """
        Запуск функции контроля ОЗУ в отдельном потоке.
        :return: None
        """
        if self.ram_control_thread is None:
            self.run_ram_control_thread = True
            self.ram_control_thread = threading.Thread(target=self._ram_control, daemon=True)
            self.ram_control_thread.start()

    def _run_first_start(self):
        """
        Первичное распределение воркеров по ядрам CPU.
        :return: None
        """
        if not self.first_start:
            # Распределяем сервисы по ядрам при первом запуске
            if self.services_names_for_balance and self.reserved_cpu_cores:
                reserved_cpu_cores = copy.deepcopy(self.reserved_cpu_cores)
                while reserved_cpu_cores:
                    for service in self.services_names_for_balance:
                        if not reserved_cpu_cores:
                            break
                        core = reserved_cpu_cores.pop(0)
                        rabbit_service = [i for i in self.services if i.service_name == service][0]
                        rabbit_service.rabbit_workers_devices.extend([f"cpu:{core}:1"])
            # Перезапускаем сервисы
            self.restart_all_services()
            print(f'Балансировщик: первое распределение')
            for i in self.services:
                print(f'{i.service_name}: {i.rabbit_workers_devices}')
            self.first_start = True

    def _balancer(self):
        """
        Выполнение балансировки воркеров по ядрам CPU.
        :return: None
        """
        while self.run_balancer_thread:
            if not self.first_start:
                self._run_first_start()
            else:
                statistic = self.get_statistic()
                correct_statistic = {i: statistic[i] for i in statistic if i in self.services_names_for_balance}
                sorted_statistic = sorted(correct_statistic.items(), key=lambda x: x[1])
                donor: Optional[RabbitService] = None
                remove_core: Optional[str] = None
                for service_name in sorted_statistic:
                    rabbit_service = [i for i in self.services if i.service_name == service_name[0]][0]
                    for d in rabbit_service.rabbit_workers_devices:
                        _, core, _ = d.split(':')
                        core = int(core)
                        if core in self.reserved_cpu_cores:
                            if not donor:
                                donor = rabbit_service
                                remove_core = d
                needer: Optional[RabbitService] = \
                    [i for i in self.services if i.service_name == sorted_statistic[-1][0]][0]

                # Переназначаем ядро CPU
                if donor and remove_core and needer and donor != needer:
                    donor.rabbit_workers_devices.remove(remove_core)
                    needer.rabbit_workers_devices.append(remove_core)
                    print(f'Балансировщик: {remove_core} от {donor} передано {needer}')
                    print(f'{donor}: {donor.rabbit_workers_devices}')
                    print(f'{needer}: {needer.rabbit_workers_devices}')
                    # Перезапускаем сервисы
                    donor.restart()
                    needer.restart()
                else:
                    print(f'Балансировка не потребовалась')
            time.sleep(self.time_to_rebalance)

    def _run_balance_in_thread(self):
        """
        Запуск балансировщика в отдельном потоке.
        :return: None
        """
        if not self.balancer_thread and self.services_names_for_balance and self.reserved_cpu_cores:
            self.run_balancer_thread = True
            self.balancer_thread = threading.Thread(target=self._balancer, daemon=True)
            self.balancer_thread.start()


# Не использовать символ "-" в создании названий контейнеров
list_of_services = [
    {"service_name": "first_ms", "service_port": 8001, "rabbit_workers_devices": ["cpu:0:1"]},
    {"service_name": "second_ms", "service_port": 8002, "rabbit_workers_devices": ["cpu:0:1"]},
    {"service_name": "third_ms", "service_port": 8003, "rabbit_workers_devices": ["cpu:1:1"]},
    {"service_name": "fourth_ms", "service_port": 8004, "rabbit_workers_devices": ["cpu:0:1"]},
    {"service_name": "fifth_ms", "service_port": 8005, "rabbit_workers_devices": ["cpu:2:1"]},
    {"service_name": "sixth_ms", "service_port": 8006, "rabbit_workers_devices": ["cpu:3:1"]},
    {"service_name": "seventh_ms", "service_port": 8007, "rabbit_workers_devices": ["cpu:4:1"]},
    {"service_name": "eighth_ms", "service_port": 8008, "rabbit_workers_devices": ["cpu:5:1"]},
    {"service_name": "ninth_ms", "service_port": 8009, "rabbit_workers_devices": ["cpu:6:1"]},
    {"service_name": "tenth_ms", "service_port": 8010, "rabbit_workers_devices": ["cpu:7:1"]},
    {"service_name": "eleventh_ms", "service_port": 8011, "rabbit_workers_devices": ["cpu:0:1"]},
    {"service_name": "twelfth_ms", "service_port": 8012, "rabbit_workers_devices": ["cpu:0:1"],
     "workers_kwargs": {"pause_for_send_to_db": 5, "send_to_db": True}},
]
manager = RabbitServiceManager(
    rabbit_url="amqp://guest:guest@localhost:5672/",
    list_of_services=list_of_services,
    services_names_for_balance=["third_ms", "fifth_ms", "sixth_ms", "seventh_ms", "eighth_ms", "ninth_ms", "tenth_ms"],
    reserved_cpu_cores=[8, 9, 10, 11],
    time_to_rebalance=3600 * 1
)


manager.run_all_services()

while True:
    print(manager.get_statistic())
    time.sleep(3600)

manager.stop_all_services()
