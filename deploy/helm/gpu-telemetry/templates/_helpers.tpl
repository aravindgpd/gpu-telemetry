{{/*
Expand the name of the chart.
*/}}
{{- define "gpu-telemetry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gpu-telemetry.fullname" -}}
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
Chart label.
*/}}
{{- define "gpu-telemetry.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "gpu-telemetry.labels" -}}
helm.sh/chart: {{ include "gpu-telemetry.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{/*
Selector labels for a component (pass component name as .component).
*/}}
{{- define "gpu-telemetry.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gpu-telemetry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
PostgreSQL DSN — uses the Bitnami subchart service name when enabled,
otherwise uses externalDatabase values.
*/}}
{{- define "gpu-telemetry.databaseURL" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "postgres://%s:%s@%s-postgresql:5432/%s" .Values.postgresql.auth.username .Values.postgresql.auth.password .Release.Name .Values.postgresql.auth.database }}
{{- else }}
{{- printf "postgres://%s:%s@%s:%d/%s" .Values.externalDatabase.username .Values.externalDatabase.password .Values.externalDatabase.host (.Values.externalDatabase.port | int) .Values.externalDatabase.database }}
{{- end }}
{{- end }}

{{/*
MQ broker address (service name:port).
*/}}
{{- define "gpu-telemetry.mqAddress" -}}
{{- printf "%s-messagequeue:%d" (include "gpu-telemetry.fullname" .) (.Values.messagequeue.service.port | int) }}
{{- end }}

{{/*
Resolved PostgreSQL connection coordinates — switches between the in-chart
Bitnami subchart and externalDatabase config.
*/}}
{{- define "gpu-telemetry.postgres.host" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "%s-postgresql" .Release.Name -}}
{{- else -}}
{{- .Values.externalDatabase.host -}}
{{- end -}}
{{- end }}

{{- define "gpu-telemetry.postgres.port" -}}
{{- if .Values.postgresql.enabled -}}
5432
{{- else -}}
{{- .Values.externalDatabase.port -}}
{{- end -}}
{{- end }}

{{- define "gpu-telemetry.postgres.user" -}}
{{- if .Values.postgresql.enabled -}}
{{- .Values.postgresql.auth.username -}}
{{- else -}}
{{- .Values.externalDatabase.username -}}
{{- end -}}
{{- end }}

{{- define "gpu-telemetry.postgres.database" -}}
{{- if .Values.postgresql.enabled -}}
{{- .Values.postgresql.auth.database -}}
{{- else -}}
{{- .Values.externalDatabase.database -}}
{{- end -}}
{{- end }}

{{/*
Init container that blocks pod startup until PostgreSQL accepts connections.
Reuses the postgresql.image so the node already has it cached (no extra pull).
Use:
  spec:
    initContainers:
      {{- include "gpu-telemetry.waitForPostgres" . | nindent 8 }}
*/}}
{{- define "gpu-telemetry.waitForPostgres" -}}
- name: wait-for-postgres
  image: "{{ .Values.postgresql.image.registry }}/{{ .Values.postgresql.image.repository }}:{{ .Values.postgresql.image.tag }}"
  imagePullPolicy: IfNotPresent
  command:
    - /bin/sh
    - -c
    - |
      set -e
      host="{{ include "gpu-telemetry.postgres.host" . }}"
      port="{{ include "gpu-telemetry.postgres.port" . }}"
      user="{{ include "gpu-telemetry.postgres.user" . }}"
      db="{{ include "gpu-telemetry.postgres.database" . }}"
      echo "waiting for postgres at ${host}:${port} (db=${db}, user=${user})"
      until pg_isready -h "${host}" -p "${port}" -U "${user}" -d "${db}" -t 2; do
        echo "  postgres not ready, retrying in 2s..."
        sleep 2
      done
      echo "postgres is ready"
  resources:
    requests:
      cpu: 10m
      memory: 32Mi
    limits:
      cpu: 100m
      memory: 64Mi
{{- end }}
