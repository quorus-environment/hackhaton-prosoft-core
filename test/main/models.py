from django.db import models


class Controller(models.Model):
    """Пока хз нужно ли это вообще"""
    name = models.CharField(max_length=200)
    ip = models.CharField(max_length=200)

    def __str__(self):
        return str(self.name)


class Client(models.Model):
    """Модель Client, через нее все ищем скорее всего"""

    address = models.CharField(max_length=200)
    name = models.CharField(max_length=200)
    controller = models.ForeignKey(
        Controller,
        on_delete=models.CASCADE
    )

    def __str__(self):
        return str(self.id)

class Server(models.Model):
    """Модель Server, через нее все ищем скорее всего"""

    address = models.CharField(max_length=200)
    name = models.CharField(max_length=200)
    controller = models.ForeignKey(
        Controller,
        on_delete=models.CASCADE
    )

    def __str__(self):
        return str(self.id)


class Werehouse(models.Model):
    """Модель Werehouse, через нее все ищем скорее всего"""
    name = models.CharField(max_length=200)

    client = models.ForeignKey(
        Client,
        on_delete=models.SET_NULL,
        null=True
    )

    server = models.ForeignKey(
        Server,
        on_delete=models.SET_NULL,
        null=True
    )

    controller = models.ForeignKey(
        Controller,
        on_delete=models.CASCADE
    )

    def __str__(self):
        return str(self.id)
