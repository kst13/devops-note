{{- define "was-order.image" -}}
{{ .Values.image.registry }}/{{ .Values.image.name }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "was-order.labels" -}}
app: was-order
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
