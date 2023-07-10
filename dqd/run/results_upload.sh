current_date="$(date +%F)"
new_output_folder="output-${current_date}"

mv output "${new_output_folder}"
gcloud auth activate-service-account --key-file gcp_key.json
gsutil -m cp -r "${new_output_folder}" gs://dqd_output