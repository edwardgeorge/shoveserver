from StringIO import StringIO


def server_request(server, request):
    request = StringIO(request)
    respio = StringIO()
    server.handle_connection(request, respio)
    return respio.getvalue()
