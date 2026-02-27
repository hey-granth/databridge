"""URL routing for the pipelines API."""

from django.urls import include, path
from rest_framework.routers import DefaultRouter

from pipelines.api.views import PipelineRunViewSet, PipelineViewSet

router = DefaultRouter()
router.register(r"pipelines", PipelineViewSet, basename="pipeline")
router.register(r"runs", PipelineRunViewSet, basename="run")

urlpatterns = [
    path("", include(router.urls)),
]
