{{- define "imposm.name" -}}imposm-operator{{- end }}

{{- define "imposm.fullname" -}}
{{- if contains "imposm-operator" .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-imposm-operator" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "imposm.labels" -}}
app.kubernetes.io/name: imposm-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "imposm.controllerImage" -}}
{{ .Values.image.controller.repository }}:{{ .Values.image.controller.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "imposm.runnerImage" -}}
{{ .Values.image.runner.repository }}:{{ .Values.image.runner.tag | default .Chart.AppVersion }}
{{- end }}


{{- define "imposm.watchNamespace" -}}
{{- if .Values.watchNamespace -}}
{{ .Values.watchNamespace }}
{{- else if eq .Values.rbac.scope "namespace" -}}
{{ .Release.Namespace }}
{{- end -}}
{{- end }}

{{- define "imposm.validate" -}}
{{- if not (has .Values.mode (list "crd" "configmap")) -}}
{{- fail "mode must be crd or configmap" -}}
{{- end -}}
{{- if and (eq .Values.rbac.scope "namespace") .Values.watchNamespace (ne .Values.watchNamespace .Release.Namespace) -}}
{{- fail "rbac.scope=namespace only grants access to the release namespace; watchNamespace must be empty or equal to it" -}}
{{- end -}}
{{- end }}

{{- define "imposm.metricsSecure" -}}
{{- if eq (toString .Values.metrics.secure) "auto" -}}
{{- ternary "true" "false" (eq .Values.rbac.scope "cluster") -}}
{{- else -}}
{{- ternary "true" "false" (eq (toString .Values.metrics.secure) "true") -}}
{{- end -}}
{{- end }}

{{- define "imposm.metricsPort" -}}
{{- ternary 8443 8080 (eq (include "imposm.metricsSecure" .) "true") -}}
{{- end }}

{{- define "imposm.promOperator" -}}
{{- $v := toString .value -}}
{{- if or (eq $v "true") (and (eq $v "auto") (.root.Capabilities.APIVersions.Has "monitoring.coreos.com/v1/ServiceMonitor")) -}}true{{- end -}}
{{- end }}

{{- define "imposm.controllerSelector" -}}
app.kubernetes.io/name: imposm-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end }}

{{/* A Go duration ("24h", "1h30m", "38s", "1.5h") in whole seconds, rounded
up; anything else fails the render. */}}
{{- define "imposm.durationSeconds" -}}
{{- $units := dict "h" 3600.0 "m" 60.0 "s" 1.0 "ms" 0.001 "us" 0.000001 "µs" 0.000001 "ns" 0.000000001 -}}
{{- $total := 0.0 -}}
{{- range regexFindAll "[0-9.]+(ns|us|µs|ms|s|m|h)" (toString .) -1 -}}
{{- $n := regexFind "^[0-9.]+" . | float64 -}}
{{- $total = addf $total (mulf $n (get $units (regexReplaceAll "^[0-9.]+" . ""))) -}}
{{- end -}}
{{- if or (not (regexMatch "^([0-9]+(\\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$" (toString .))) (lt $total 1.0) -}}
{{- fail (printf "invalid duration %q (want e.g. 36h, 1h30m, 90s)" (toString .)) -}}
{{- end -}}
{{- ceil $total | int64 -}}
{{- end }}

{{/* NetworkPolicyConfig for the controller's per-sync runner policies
(internal/controller/netpol.go). */}}
{{- define "imposm.netpolConfig" -}}
{{- $np := .Values.networkPolicy -}}
{{- dict "ingress" $np.ingress "egress" $np.egress "dns" $np.dns "nonPublicIPv4" $np.nonPublicIPv4 "nonPublicIPv6" $np.nonPublicIPv6 "internal" ($np.runner.internalTo | default list) "extraEgress" ($np.runner.extraEgress | default list) | toYaml -}}
{{- end }}

