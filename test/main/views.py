from django.http import JsonResponse

from main.models import Client, Server, Werehouse

def get_data(request):
    print(len(Client.objects.all()))
    print(len(Server.objects.all()))
    test = Client.objects.all()[1]
    r = Werehouse.objects.all()[0]
    print(r.server)
    print(r.client)
    return JsonResponse(list(Client.objects.values()), safe=False)
