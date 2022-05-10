SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, 
  num_of_participants, participants_with_EHR, participants_with_measurement, 
  participants_with_ehr_height, height_rate, participants_with_ehr_weight, weight_rate,
  participants_with_ehr_bmi, bmi_rate, participants_with_ehr_heart_rate, heart_rate
FROM `{{curation_project}}.{{ehr_ops_dataset}}.snapshot_physical_meas`