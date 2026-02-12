feature=$1

params=(
  "./ d gpAdminLogs"
  "gpdb_src/gpAux/gpdemo/datadirs/ d log"
  "gpdb_src/gpAux/gpdemo/datadirs/ d pg_log"
)
for param in "${params[@]}"; do
  read -r path type name <<< "$param"
  [ -d "$path" ] && find "$path" -name "$name" -type "$type" \
    -exec tar -rf "/logs/behave_${feature}_${name}.tar" "{}" \;
done
chmod -R a+rwX /logs
