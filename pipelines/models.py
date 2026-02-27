from django.db import models


class Pipeline(models.Model):
    """Stores a named ETL pipeline with its JSON configuration."""

    name = models.CharField(max_length=255, unique=True)
    configuration = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return self.name


class PipelineRun(models.Model):
    """Records a single execution of a Pipeline."""

    class Status(models.TextChoices):
        PENDING = "pending"
        COMPLETED = "completed"
        FAILED = "failed"

    pipeline = models.ForeignKey(
        Pipeline,
        on_delete=models.CASCADE,
        related_name="runs",
    )
    input_file = models.FileField(upload_to="uploads/")
    output_file = models.FileField(upload_to="outputs/", null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
    )
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"Run #{self.pk} — {self.pipeline.name} ({self.status})"


class OutputData(models.Model):
    """Stores transformed rows when destination is 'database'."""

    pipeline_run = models.ForeignKey(
        PipelineRun,
        on_delete=models.CASCADE,
        related_name="output_rows",
    )
    data = models.JSONField()

    def __str__(self) -> str:
        return f"OutputData #{self.pk} (Run #{self.pipeline_run_id})"
