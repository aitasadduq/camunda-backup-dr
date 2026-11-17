{{/*
Expand the name of the chart.
*/}}
{{- define "camunda-backup-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "camunda-backup-controller.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "camunda-backup-controller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "camunda-backup-controller.labels" -}}
helm.sh/chart: {{ include "camunda-backup-controller.chart" . }}
{{ include "camunda-backup-controller.selectorLabels" . }}
app.kubernetes.io/version: {{ include "camunda-backup-controller.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
The image tag to deploy: image.tag when set, otherwise the release this chart
was packaged for. A chart taken from a git checkout was packaged for none, and
guessing (or falling back to "latest") would deploy an image nobody chose.
*/}}
{{- define "camunda-backup-controller.imageTag" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- if or (not $tag) (eq $tag "unreleased") -}}
{{- fail "image.tag is required when installing this chart from a git checkout: pass --set image.tag=vX.Y.Z, or install the packaged chart attached to the GitHub release, which defaults to its own version" -}}
{{- end -}}
{{- $tag -}}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "camunda-backup-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "camunda-backup-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "camunda-backup-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "camunda-backup-controller.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
