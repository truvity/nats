{{/*
The object's name: `alerts.name`, else the release's full name with an
`-alerts` suffix. The same rule the upstream chart applies to its own
objects, so a second release in a namespace does not collide.
*/}}
{{- define "nats-broker.alerts.name" -}}
{{- if .Values.alerts.name -}}
{{- .Values.alerts.name -}}
{{- else -}}
{{- printf "%s-alerts" (include "nats-broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
The upstream chart's own naming (nats.fullname / nats.namespace), computed
from the same inputs, because the alerts must name the PodMonitor job and
the namespace the broker's metrics actually carry. Both are overridable
(`alerts.job`, `alerts.memory.namespace`) for an install that renames them
some other way.
*/}}
{{- define "nats-broker.fullname" -}}
{{- $nats := .Values.nats | default dict -}}
{{- if $nats.fullnameOverride -}}
{{- $nats.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "nats" $nats.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "nats-broker.namespace" -}}
{{- $nats := .Values.nats | default dict -}}
{{- default .Release.Namespace $nats.namespaceOverride -}}
{{- end -}}

{{/*
A rule's labels: the common labels with this rule's severity on top, so
`severity` can never be shadowed by a common label. An optional `omit`
names one common label to leave off (`alerts.keepClusterLabel`).
*/}}
{{- define "nats-broker.alerts.labels" -}}
{{- $severity := .severity -}}
{{- $common := deepCopy .root.Values.alerts.commonLabels -}}
{{- if .omit -}}{{- $_ := unset $common .omit -}}{{- end -}}
{{- $labels := merge (dict "severity" $severity) $common -}}
{{- toYaml $labels -}}
{{- end -}}

{{/*
A rule's runbook link, or nothing when no base URL is configured. An
annotation with an empty value reads as a broken link, so it is omitted
rather than rendered blank.
*/}}
{{- define "nats-broker.alerts.runbook" -}}
{{- if .root.Values.alerts.runbookBaseUrl -}}
runbook_url: {{ printf "%s/%s" (trimSuffix "/" .root.Values.alerts.runbookBaseUrl) .alert | quote }}
{{- end -}}
{{- end -}}

{{/*
A deadman for a series that must exist, per cluster.

A bare `absent(sel)` is true only when NO series matches, and a metrics
store can hold several clusters' series: the series vanishing from one
cluster leaves the others' behind, `absent()` stays false, and the outage is
silent. This renders, for a non-empty `clusterLabel`:

  (group by (<cluster>) (max_over_time(sel[<absentLookback>]))
     unless group by (<cluster>) (sel))
  or (absent(sel) unless on() group(max_over_time(sel[<absentLookback>])))

The first line fires once per cluster that HAD the series within
`absentLookback` and no longer does; its result carries that label, which
the alert keeps (the rule omits the cluster label from its static labels).
The second keeps the whole-store `absent()` for "never existed / everything
gone", and is silenced while the first can still see the series, so one
outage is one alert. Empty `clusterLabel` renders the bare `absent()`.
*/}}
{{- define "nats-broker.alerts.absentGuard" -}}
{{- $r := .root -}}
{{- if $r.Values.alerts.clusterLabel -}}
{{- $l := $r.Values.alerts.clusterLabel -}}
{{- $lb := $r.Values.alerts.absentLookback -}}
(group by ({{ $l }}) (max_over_time({{ .sel }}[{{ $lb }}])) unless group by ({{ $l }}) ({{ .sel }})) or (absent({{ .sel }}) unless on() group(max_over_time({{ .sel }}[{{ $lb }}])))
{{- else -}}
absent({{ .sel }})
{{- end -}}
{{- end -}}

{{/*
The annotation suffix naming the cluster a per-cluster absent alert is
about; empty for the whole-store alert, whose result has no cluster label.
*/}}
{{- define "nats-broker.alerts.onCluster" -}}
{{- if .Values.alerts.clusterLabel -}}{{ printf "{{ with $labels.%s }} on cluster {{ . }}{{ end }}" .Values.alerts.clusterLabel }}{{- end -}}
{{- end -}}
