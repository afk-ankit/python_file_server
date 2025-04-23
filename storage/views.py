import os
from django.conf import settings
from django.http import FileResponse, Http404
from rest_framework.views import APIView


class FileDownloadView(APIView):
    def get(self, request, filename):
        file_path = os.path.join(settings.BASE_DIR, 'downloadables', filename)

        if not os.path.exists(file_path):
            raise Http404("File not found.")

        return FileResponse(open(file_path, 'rb'), as_attachment=True, filename=filename)
