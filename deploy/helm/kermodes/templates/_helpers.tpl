{{- define "kermodes.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kermodes.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "kermodes.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "kermodes.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- /* Each weir replica's address: pod n at <pod>.<headless service>. */ -}}
{{- define "kermodes.weirHosts" -}}
{{- $name := printf "%s-weir" (include "kermodes.fullname" .) -}}
{{- range $i := until (int .Values.weir.replicas) -}}
{{- if $i }},{{ end -}}
{{ $name }}-{{ $i }}.{{ $name }}.{{ $.Release.Namespace }}.svc:8731
{{- end -}}
{{- end -}}

{{- /* weir for its clients: every replica, so a client finds whichever leads. */ -}}
{{- define "kermodes.weirUrl" -}}
grpc://{{ include "kermodes.weirHosts" . }}
{{- end -}}

{{- /* weir for its replicas: each replica's own URL, in order. */ -}}
{{- define "kermodes.weirPeers" -}}
{{- range $i, $host := splitList "," (include "kermodes.weirHosts" .) -}}
{{- if $i }},{{ end }}grpc://{{ $host }}
{{- end -}}
{{- end -}}

{{- define "kermodes.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "kermodes.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}
