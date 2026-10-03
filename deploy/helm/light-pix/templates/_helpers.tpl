{{/* Rótulos comuns. */}}
{{- define "lp.labels" -}}
app.kubernetes.io/part-of: light-pix
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Ambiente de telemetria comum a todos os serviços Go. */}}
{{- define "lp.otelEnv" -}}
- { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: "http://otel-collector:4317" }
- { name: OTEL_EXPORTER_OTLP_INSECURE, value: "true" }
- { name: OTEL_METRIC_EXPORT_INTERVAL, value: "10000" }
{{- end }}

{{/* Espera o MongoDB eleger o primário antes de subir o serviço: sem isso o
     primeiro Connect falha enquanto o replica set é iniciado. */}}
{{- define "lp.waitMongo" -}}
- name: wait-mongo
  image: {{ .Values.images.mongo }}
  imagePullPolicy: {{ .Values.pullPolicy }}
  command:
    - sh
    - -c
    - 'until mongosh --host mongo-0.mongo --quiet --eval "if (!db.hello().isWritablePrimary) quit(1)"; do echo "esperando o primário do MongoDB"; sleep 2; done'
{{- end }}

{{/* Segurança padrão dos contêineres Go (imagem distroless, usuário nonroot). */}}
{{- define "lp.securityContext" -}}
runAsNonRoot: true
# A imagem declara USER por nome ("nonroot"); o Kubernetes só verifica UID numérico.
runAsUser: 65532
runAsGroup: 65532
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities: { drop: [ALL] }
{{- end }}

{{/* Sondas: /healthz diz que o processo responde; /readyz, que as dependências também. */}}
{{- define "lp.probes" -}}
livenessProbe:
  httpGet: { path: /healthz, port: http }
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: { path: /readyz, port: http }
  periodSeconds: 5
  failureThreshold: 3
startupProbe:
  httpGet: { path: /healthz, port: http }
  periodSeconds: 2
  failureThreshold: 60
{{- end }}
