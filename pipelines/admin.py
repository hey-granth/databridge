from django.contrib import admin

from pipelines.models import OutputData, Pipeline, PipelineRun


@admin.register(Pipeline)
class PipelineAdmin(admin.ModelAdmin):
    list_display = ["id", "name", "created_at"]


@admin.register(PipelineRun)
class PipelineRunAdmin(admin.ModelAdmin):
    list_display = ["id", "pipeline", "status", "created_at"]
    list_filter = ["status"]


@admin.register(OutputData)
class OutputDataAdmin(admin.ModelAdmin):
    list_display = ["id", "pipeline_run"]
