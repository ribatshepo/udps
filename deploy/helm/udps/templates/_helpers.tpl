{{/*
Expand the name of the chart.
*/}}
{{- define "udps.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "udps.fullname" -}}
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
{{- define "udps.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "udps.labels" -}}
helm.sh/chart: {{ include "udps.chart" . }}
{{ include "udps.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: seri-sa-platform
{{- end }}

{{/*
Selector labels
*/}}
{{- define "udps.selectorLabels" -}}
app.kubernetes.io/name: {{ include "udps.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: data-platform
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "udps.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "udps.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace name
*/}}
{{- define "udps.namespace" -}}
{{- default "seri-sa-platform" .Values.namespaceOverride }}
{{- end }}
