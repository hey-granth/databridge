"""DRF serializers for the pipelines API."""

from rest_framework import serializers

from pipelines.models import Pipeline, PipelineRun
from pipelines.services.config_validator import validate_config


class PipelineSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pipeline
        fields = ["id", "name", "configuration", "created_at"]
        read_only_fields = ["id", "created_at"]

    def validate_configuration(self, value):
        errors = validate_config(value)
        if errors:
            raise serializers.ValidationError(errors)
        return value


class PipelineRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = PipelineRun
        fields = [
            "id",
            "pipeline",
            "input_file",
            "output_file",
            "status",
            "error_message",
            "created_at",
        ]
        read_only_fields = fields


class PipelineRunTriggerSerializer(serializers.Serializer):
    file = serializers.FileField(required=True)
    destination = serializers.ChoiceField(
        choices=["csv", "database"],
        default="csv",
    )
