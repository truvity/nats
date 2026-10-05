now=$(date +%s)
for kind in streams consumers; do
  kubectl get "$kind" -A -o go-template='{{range .items}}{{$ns:=.metadata.namespace}}{{$n:=.metadata.name}}{{range .status.conditions}}{{if and (eq .type "Ready") (eq .reason "Errored")}}{{$ns}} {{$n}} {{.lastTransitionTime}}{{"\n"}}{{end}}{{end}}{{end}}' |
  while read -r ns name since; do
    [ -n "$ns" ] || continue
    since_epoch=$(date -u -D %Y-%m-%dT%H:%M:%S -d "${since%%.*}" +%s)
    if [ $((now - since_epoch)) -gt 300 ]; then
      echo "reaping $kind $ns/$name (Errored since $since)"
      kubectl delete -n "$ns" "$kind" "$name" --wait=false
    else
      echo "skipping $kind $ns/$name (Errored only since $since)"
    fi
  done
done
