"""DRF views for the pipelines API."""

from django.http import FileResponse
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.response import Response

from pipelines.api.serializers import (
    PipelineRunSerializer,
    PipelineRunTriggerSerializer,
    PipelineSerializer,
)
from pipelines.models import Pipeline, PipelineRun
from pipelines.services.pipeline_service import PipelineExecutionError, run_pipeline


class PipelineViewSet(
    mixins.CreateModelMixin,
    viewsets.GenericViewSet,
):
    """
    POST /api/pipelines/           — Create a pipeline
    POST /api/pipelines/{id}/run/  — Trigger a run with file upload
    """

    queryset = Pipeline.objects.all()
    serializer_class = PipelineSerializer
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    @action(
        detail=True,
        methods=["post"],
        url_path="run",
        parser_classes=[MultiPartParser, FormParser],
    )
    def run(self, request, pk=None):
        """Execute the pipeline with an uploaded file."""
        pipeline = self.get_object()

        trigger_ser = PipelineRunTriggerSerializer(data=request.data)
        if not trigger_ser.is_valid():
            return Response(
                {
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid request parameters.",
                        "details": trigger_ser.errors,
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        uploaded_file = trigger_ser.validated_data["file"]
        destination = trigger_ser.validated_data["destination"]

        try:
            pipeline_run = run_pipeline(pipeline, uploaded_file, destination)
        except PipelineExecutionError as exc:
            return Response(
                {"error": {"code": exc.code, "message": exc.message}},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            PipelineRunSerializer(pipeline_run).data, status=status.HTTP_200_OK
        )


class PipelineRunViewSet(
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    """
    GET /api/runs/{id}/           — Retrieve run status
    GET /api/runs/{id}/download/  — Download output CSV
    """

    queryset = PipelineRun.objects.all()
    serializer_class = PipelineRunSerializer

    @action(detail=True, methods=["get"], url_path="download")
    def download(self, request, pk=None):
        """Download the output file if the run produced a CSV."""
        pipeline_run = self.get_object()

        if not pipeline_run.output_file:
            return Response(
                {
                    "error": {
                        "code": "NO_OUTPUT",
                        "message": "This run has no output file.",
                    }
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        return FileResponse(
            pipeline_run.output_file.open("rb"),
            as_attachment=True,
            filename=pipeline_run.output_file.name.split("/")[-1],
        )
