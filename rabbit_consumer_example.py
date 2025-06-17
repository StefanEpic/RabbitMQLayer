import multiprocessing
import os
import re
import threading
import sys
import time
from typing import Optional, List, Callable, Tuple

from fastapi import FastAPI, HTTPException
from pika.adapters.blocking_connection import BlockingConnection
from pika.connection import URLParameters
from pika.spec import BasicProperties
from pydantic import BaseModel, validator


class RabbitMessage(BaseModel):
    text: str
    table_name: str
    material_id: int
    path_to_file: str
    result_dir: str
    result: str
    time: str


class RabbitWorkerKwargs(BaseModel):
    # Дополнительные параметры при необходимости (для каждого МС могут быть свои)
    pass


class StartRabbit(BaseModel):
    rabbit_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbit_subscribe_queue: Optional[str] = None
    rabbit_publish_queue: Optional[str] = None
    rabbit_workers_devices: List[str]
    rabbit_workers_kwargs: Optional[RabbitWorkerKwargs] = None

    @validator("rabbit_workers_devices", each_item=True)
    def validate_device_format(cls, v):
        pattern = rf"^(cpu|cuda):(\d+):([1-9])$"
        error_mgs = (
            f"Значение должно иметь формат '[cpu|cuda]:X:Y', где: "
            f"X - это номер ядра/номер видеокарты и должен быть в диапазоне 0<=X<os.cpu_count(), а "
            f"Y - количество потоков и должно быть в диапазоне 1<=Y<=9."
        )
        if not re.fullmatch(pattern, v):
            raise ValueError(error_mgs)
        device, core_num, threads = v.split(":")
        if not 0 <= int(core_num) < os.cpu_count():
            raise ValueError(error_mgs)
        return v


def rabbit_func(
        task: RabbitMessage, service_device: str, kwargs: RabbitWorkerKwargs
) -> Tuple[bool, RabbitMessage, Optional[int]]:
    """
    Функция - обертка для работы с реббитом.
    Возвращает кортеж для обработки воркером реббита:
    (флаг отправить ли ответ, тело сообщения, приоритет сообщения в случае необходимости изменения).
    """
    result, time = some_base_worker_func(
        input_text=task.text,
        table_name=task.table_name,
        material_id=task.material_id,
        service_device=service_device,
        kwargs=kwargs,
    )
    task.result = result
    task.time = time
    return True, task, None


class RabbitWorker:
    """
    Воркер для работы с RammitMQ.
    """

    def __init__(
            self,
            subscribe_queue: str,
            publish_queue: str,
            connection_params: URLParameters,
            worker_device: str,
            function: Callable,
            worker_kwargs: RabbitWorkerKwargs = None,
            consumer_tag: str = None,
    ):
        self.connection_params = connection_params
        self.subscribe_queue = subscribe_queue
        self.publish_queue = publish_queue
        self.function = function
        self.worker_device = worker_device
        self.worker_kwargs: Optional[RabbitWorkerKwargs] = worker_kwargs
        self.consumer_tag = consumer_tag
        self.connection = None
        self.channel = None
        self.heartbeat_thread = None

    def _create_connection(self):
        """
        Создание канала прослушивания RabbitMQ.
        """
        while True:
            try:
                if not self.connection or self.connection.is_closed:
                    if not self.channel or self.channel.is_closed:
                        self.connection = BlockingConnection(self.connection_params)
                        self.channel = self.connection.channel()
                        self.channel.basic_qos(prefetch_count=1)
                        self.channel.basic_consume(
                            queue=self.subscribe_queue,
                            on_message_callback=self._extract_message,
                            consumer_tag=self.consumer_tag,
                        )
                        print(f"Запуск прослушивания очереди {self.subscribe_queue}...")
                        return
            except Exception as e:
                print("Ошибка создания канала связи с RabbitMQ:", e, flush=True)
            time.sleep(10)

    def _heartbeat(self):
        """
        Отправка heartbeat для RabbitMQ в отдельном потоке для поддержки канала открытым.
        """
        while True:
            try:
                self.connection.process_data_events()
            except Exception as e:
                print(e, flush=True)
                print("Потеряно соединение для heartbeat...", flush=True)
            time.sleep(10)

    def _run_heartbeat(self):
        """
        Запуск отправки heartbeat для RabbitMQ в отдельном потоке.
        """
        if not self.heartbeat_thread:
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat, name=f"{self.consumer_tag}_heartbeat", daemon=True
            )
            self.heartbeat_thread.start()

    def _send_to_rabbit(self, data: str, properties: BasicProperties) -> None:
        """
        Отправить data в очередь self.publish_queue RabbitMQ.
        """
        while True:
            try:
                with BlockingConnection(self.connection_params) as send_connection:
                    with send_connection.channel() as send_channel:
                        send_channel.basic_publish(
                            exchange="", routing_key=self.publish_queue, body=data, properties=properties
                        )
                return
            except Exception as e:
                print(e, flush=True)
                print(f"Повторная попытка отправки ответа в очередь {self.publish_queue}...", flush=True)
            time.sleep(10)

    def _extract_message(self, ch, method, input_properties, body: bytes) -> None:
        """
        Обработка результатов из очереди и направление результатов в RabbitMQ.
        """
        try:
            task = RabbitMessage.parse_raw(body.decode("utf-8"))
            print(f"Задача получена: {task.text[:15]}...", flush=True)
            need_to_send, message, priority = self.function(
                task=task,
                service_device=self.worker_device,
                kwargs=self.worker_kwargs,
            )
            # Отправка сообщения при включенном флаге и наличии очереди
            if need_to_send and self.publish_queue:
                # Переопределение приоритета при необходимости
                input_priority = 2
                if input_properties:
                    if input_properties.priority:
                        input_priority = input_properties.priority
                properties = BasicProperties(delivery_mode=2, priority=input_priority)
                if priority:
                    properties = BasicProperties(delivery_mode=2, priority=priority)
                # Отправка сообщения
                self._send_to_rabbit(data=message.json().encode("utf-8"), properties=properties)
            print(f"Задача выполнена: {task.text[:15]}...", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as error:
            print(error, flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            sys.exit(1)

    def run(self) -> None:
        """
        Начать прослушивание очереди self.subcribe_queue.
        """
        while True:
            try:
                self._create_connection()
                self._run_heartbeat()
                self.channel.start_consuming()
            except Exception as e:
                print("Ошибка прослушивания очереди:", e)
            time.sleep(10)


class RabbitManager:
    """
    Менеджер для создания нескольких воркеров прослушивания очереди RabbitMQ.
    """

    def __init__(self, worker_function: Callable, consumer_tag: str = None):
        self.rabbit_params: Optional[URLParameters] = None
        self.subscribe_queue: Optional[str] = None
        self.publish_queue: Optional[str] = None
        self.worker_function = worker_function
        self.workers_kwargs: Optional[RabbitWorkerKwargs] = None
        # Список [RabbitWorker, процесс в котором запущен RabbitWorker.run(), номер ядра CPU]
        self.running_workers: List[Tuple[RabbitWorker, multiprocessing.Process, Optional[int]]] = []
        self.workers_devices: List[str] = []
        self.consumer_tag: str = consumer_tag
        self.restart_workers_thread: Optional[threading.Thread] = None
        self.run_restart_workers_thread: bool = False

    def set_settings(self, settings: StartRabbit):
        """
        Утсановить настройки обработки.
        """
        self.rabbit_params = URLParameters(settings.rabbit_url)
        self.subscribe_queue = settings.rabbit_subscribe_queue
        self.publish_queue = settings.rabbit_publish_queue
        self.workers_devices = settings.rabbit_workers_devices
        self.workers_kwargs = settings.rabbit_workers_kwargs

    def _check_rabbit_connection(self) -> bool:
        """
        Проверка наличия подключения к RabbitMQ.
        """
        try:
            connection = BlockingConnection(self.rabbit_params)
            connection.close()
            return True
        except Exception as e:
            print(str(e), flush=True)
            return False

    def _check_rabbit_queue(self, queue_name: str) -> bool:
        """
        Проверка наличия очереди RabbitMQ.
        """
        try:
            connection = BlockingConnection(self.rabbit_params)
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, passive=True)
            connection.close()
            return True
        except Exception as e:
            print(str(e), flush=True)
            return False

    def _check_workers(self) -> None:
        """
        Проверка здоровья воркеров и перезапуск в случае падения воркера.
        """
        while self.run_restart_workers_thread:
            for w in self.running_workers:
                rabbit_worker = w[0]
                rabbit_proces = w[1]
                rabbit_proces_num_core = w[2]
                if not rabbit_proces.is_alive() and rabbit_proces.exitcode == 1:
                    self.running_workers.remove(w)
                    rabbit_proces.kill()
                    w = multiprocessing.Process(target=rabbit_worker.run, daemon=True)
                    w.start()
                    if rabbit_proces_num_core:
                        os.sched_setaffinity(w.pid, [rabbit_proces_num_core])
                    self.running_workers.append((rabbit_worker, w, rabbit_proces_num_core))
                    print(f"Воркер {rabbit_worker} поднят после ошибки", flush=True)
            time.sleep(10)

    def _restart_workers_if_errors(self) -> None:
        """
        Запуск проверки здоровья воркеров в отдельном потоке.
        """
        if not self.restart_workers_thread:
            self.run_restart_workers_thread = True
            self.restart_workers_thread = threading.Thread(target=self._check_workers)
            self.restart_workers_thread.start()

    def run_workers(self) -> str:
        """
        Запуск работы воркеров.
        """
        if not self._check_rabbit_connection():
            print("Отсутствует подключение к RabbitMQ", flush=True)
            return "Отсутствует подключение к RabbitMQ"
        if not self._check_rabbit_queue(queue_name=self.subscribe_queue):
            print(f"В RabbitMQ отсутствует очередь {self.subscribe_queue}", flush=True)
            return f"В RabbitMQ отсутствует очередь {self.subscribe_queue}"
        if self.publish_queue:
            if not self._check_rabbit_queue(queue_name=self.publish_queue):
                print(f"В RabbitMQ отсутствует очередь {self.publish_queue}", flush=True)
                return f"В RabbitMQ отсутствует очередь {self.publish_queue}"

        if self.running_workers:
            return f"В настоящий момент уже создано {len(self.running_workers)} воркеров"

        if not self.running_workers and self.workers_devices:
            for worker in self.workers_devices:
                device_type, number, instances = worker.split(":")
                number = int(number)
                instances = int(instances)
                for n in range(instances):
                    if device_type == "cpu":
                        worker_device = device_type
                        cpu_core_num = number
                    else:
                        worker_device = f"{device_type}:{number}"
                        cpu_core_num = None
                    rabbit_worker = RabbitWorker(
                        subscribe_queue=self.subscribe_queue,
                        publish_queue=self.publish_queue,
                        connection_params=self.rabbit_params,
                        worker_device=worker_device,
                        function=self.worker_function,
                        worker_kwargs=self.workers_kwargs,
                        consumer_tag=self.consumer_tag,
                    )
                    w = multiprocessing.Process(target=rabbit_worker.run, daemon=True)
                    w.start()
                    if cpu_core_num:
                        os.sched_setaffinity(w.pid, [cpu_core_num])
                    self.running_workers.append((rabbit_worker, w, cpu_core_num))
            msg = f"Процесс прослушивание очереди {self.subscribe_queue} запущен c {len(self.running_workers)} воркерами..."
            print(msg, flush=True)
            self._restart_workers_if_errors()
            return msg
        else:
            return "Не переданы параметры запуска воркеров"

    def stop_workers(self) -> str:
        """
        Удаление воркеров.
        """
        if self.running_workers:
            msg = f"Процесс прослушивания очереди {self.subscribe_queue} остановлен..."
            for w in self.running_workers:
                w[1].kill()
            self.rabbit_params = None
            self.subscribe_queue = None
            self.publish_queue = None
            self.workers_kwargs = None
            self.running_workers = []
            self.workers_devices = []
            self.run_restart_workers_thread = False
            self.restart_workers_thread = None
            print(msg, flush=True)
            return msg
        return "В настоящее время нет запущенных воркеров"


rabbit_manager = RabbitManager(worker_function=rabbit_func, consumer_tag="consumer")
app = FastAPI(title="Consumer REST Example")


@app.post("/run_rabbit_workers", response_model=str)
async def run_rabbit_workers(task: StartRabbit):
    try:
        rabbit_manager.set_settings(settings=task)
        return rabbit_manager.run_workers()
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.post("/stop_rabbit_workers", response_model=str)
async def stop_rabbit_workers():
    return rabbit_manager.stop_workers()
