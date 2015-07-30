from ..schema_base import *

ioevents_schema = Schema("ioevents", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    big_dt("charttime"),
    num("elemid"),
    id_("altid"),
    big_dt("realtime"),
    id_("cgid"),
    id_("cuid"),
    num("volume"),
    cat("volumeuom"),
    num("unitshung"),
    cat("unitshunguom"),
    num("newbottle"),
    cat("stopped"),
    cat("estimate")
],
options={
    'db_schema': 'mimic2v26'
})

icd9_schema = Schema("icd9", [
    id_("subject_id"),
    id_("hadm_id"),
    num("sequence"),
    cat("code"),
    cat("description")
],
options={
    'db_schema': 'mimic2v26'
})

icustay_days_schema = Schema("icustay_days", [
    num("icustay_id"),
    num("subject_id"),
    num("seq"),
    big_dt("begintime"),
    big_dt("endtime"),
    cat("first_day_flg"),
    cat("last_day_flg")
],
options={
    'db_schema': 'mimic2v26'
})

a_chartdurations_schema = Schema("a_chartdurations", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    num("elemid"),
    big_dt("starttime"),
    big_dt("startrealtime"),
    big_dt("endtime"),
    id_("cuid"),
    num("duration")
],
options={
    'db_schema': 'mimic2v26'
})

a_meddurations_schema = Schema("a_meddurations", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    num("elemid"),
    big_dt("starttime"),
    big_dt("startrealtime"),
    big_dt("endtime"),
    id_("cuid"),
    num("duration")
],
options={
    'db_schema': 'mimic2v26'
})

noteevents_schema = Schema("noteevents", [
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    num("elemid"),
    big_dt("charttime"),
    big_dt("realtime"),
    id_("cgid"),
    cat("correction"),
    id_("cuid"),
    cat("category"),
    cat("title"),
    cat("text"),
    cat("exam_name"),
    cat("patient_info")
],
options={
    'db_schema': 'mimic2v26'
})

demographicevents_schema = Schema("demographicevents", [
    id_("subject_id"),
    id_("hadm_id"),
    id_("itemid")
],
options={
    'db_schema': 'mimic2v26'
})

drgevents_schema = Schema("drgevents", [
    id_("subject_id"),
    id_("hadm_id"),
    id_("itemid"),
    num("cost_weight")
],
options={
    'db_schema': 'mimic2v26'
})

admissions_schema = Schema("admissions", [
    id_("hadm_id"),
    id_("subject_id"),
    big_dt("admit_dt"),
    big_dt("disch_dt")
],
options={
    'db_schema': 'mimic2v26'
})

d_meditems_schema = Schema("d_meditems", [
    id_("itemid"),
    cat("label")
],
options={
    'db_schema': 'mimic2v26'
})

db_schema_schema = Schema("db_schema", [
    big_dt("created_dt"),
    cat("created_by"),
    big_dt("updated_dt"),
    cat("updated_by"),
    big_dt("schema_dt"),
    cat("version"),
    cat("comments")
],
options={
    'db_schema': 'mimic2v26'
})

deliveries_schema = Schema("deliveries", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("ioitemid"),
    big_dt("charttime"),
    num("elemid"),
    id_("cgid"),
    id_("cuid"),
    cat("site"),
    num("rate"),
    cat("rateuom")
],
options={
    'db_schema': 'mimic2v26'
})

poe_order_schema = Schema("poe_order", [
    id_("poe_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    big_dt("start_dt"),
    big_dt("stop_dt"),
    big_dt("enter_dt"),
    cat("medication"),
    cat("procedure_type"),
    cat("status"),
    cat("route"),
    cat("frequency"),
    cat("dispense_sched"),
    cat("iv_fluid"),
    cat("iv_rate"),
    cat("infusion_type"),
    cat("sliding_scale"),
    num("doses_per_24hrs"),
    num("duration"),
    cat("duration_intvl"),
    num("expiration_val"),
    cat("expiration_unit"),
    big_dt("expiration_dt"),
    cat("label_instr"),
    cat("additional_instr"),
    cat("md_add_instr"),
    cat("rnurse_add_instr")
],
options={
    'db_schema': 'mimic2v26'
})

d_chartitems_detail_schema = Schema("d_chartitems_detail", [
    cat("label"),
    cat("label_lower"),
    num("itemid"),
    cat("category"),
    cat("description"),
    cat("value_type"),
    cat("value_column"),
    num("rows_num"),
    num("subjects_num"),
    num("chart_vs_realtime_delay_mean"),
    num("chart_vs_realtime_delay_stddev"),
    num("value1_uom_num"),
    cat("value1_uom_has_nulls"),
    cat("value1_uom_sample1"),
    cat("value1_uom_sample2"),
    num("value1_distinct_num"),
    cat("value1_has_nulls"),
    cat("value1_sample1"),
    cat("value1_sample2"),
    num("value1_length_min"),
    num("value1_length_max"),
    num("value1_length_mean"),
    num("value1num_min"),
    num("value1num_max"),
    num("value1num_mean"),
    num("value1num_stddev"),
    num("value2_uom_num"),
    cat("value2_uom_has_nulls"),
    cat("value2_uom_sample1"),
    cat("value2_uom_sample2"),
    num("value2_distinct_num"),
    cat("value2_has_nulls"),
    cat("value2_sample1"),
    cat("value2_sample2"),
    num("value2_length_min"),
    num("value2_length_max"),
    num("value2_length_mean"),
    num("value2num_min"),
    num("value2num_max"),
    num("value2num_mean"),
    num("value2num_stddev")
],
options={
    'db_schema': 'mimic2v26'
})

d_labitems_schema = Schema("d_labitems", [
    id_("itemid"),
    cat("test_name"),
    cat("fluid"),
    cat("category"),
    cat("loinc_code"),
    cat("loinc_description")
],
options={
    'db_schema': 'mimic2v26'
})

d_caregivers_schema = Schema("d_caregivers", [
    id_("cgid"),
    cat("label")
],
options={
    'db_schema': 'mimic2v26'
})

additives_schema = Schema("additives", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    id_("ioitemid"),
    big_dt("charttime"),
    num("elemid"),
    id_("cgid"),
    id_("cuid"),
    num("amount"),
    cat("doseunits"),
    cat("route")
],
options={
    'db_schema': 'mimic2v26'
})

medevents_schema = Schema("medevents", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    big_dt("charttime"),
    num("elemid"),
    big_dt("realtime"),
    id_("cgid"),
    id_("cuid"),
    num("volume"),
    num("dose"),
    cat("doseuom"),
    id_("solutionid"),
    num("solvolume"),
    cat("solunits"),
    cat("route"),
    cat("stopped")
],
options={
    'db_schema': 'mimic2v26'
})

totalbalevents_schema = Schema("totalbalevents", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    big_dt("charttime"),
    num("elemid"),
    big_dt("realtime"),
    id_("cgid"),
    id_("cuid"),
    num("pervolume"),
    num("cumvolume"),
    cat("accumperiod"),
    cat("approx"),
    num("reset"),
    cat("stopped")
],
options={
    'db_schema': 'mimic2v26'
})

parameter_mapping_schema = Schema("parameter_mapping", [
    cat("param1_str"),
    num("param1_num"),
    cat("category"),
    cat("param2_str"),
    num("param2_num"),
    num("order_num"),
    cat("valid_flg"),
    cat("comments")
],
options={
    'db_schema': 'mimic2v26'
})

a_iodurations_schema = Schema("a_iodurations", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    num("elemid"),
    big_dt("starttime"),
    big_dt("startrealtime"),
    big_dt("endtime"),
    id_("cuid"),
    num("duration")
],
options={
    'db_schema': 'mimic2v26'
})

censusevents_schema = Schema("censusevents", [
    id_("census_id"),
    id_("subject_id"),
    big_dt("intime"),
    big_dt("outtime"),
    id_("careunit"),
    id_("destcareunit"),
    cat("dischstatus"),
    num("los"),
    id_("icustay_id")
],
options={
    'db_schema': 'mimic2v26'
})

d_chartitems_schema = Schema("d_chartitems", [
    id_("itemid"),
    cat("label"),
    cat("category"),
    cat("description")
],
options={
    'db_schema': 'mimic2v26'
})

icustayevents_schema = Schema("icustayevents", [
    id_("icustay_id"),
    id_("subject_id"),
    big_dt("intime"),
    big_dt("outtime"),
    num("los"),
    id_("first_careunit"),
    id_("last_careunit")
],
options={
    'db_schema': 'mimic2v26'
})

labevents_schema = Schema("labevents", [
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    id_("itemid"),
    big_dt("charttime"),
    cat("value"),
    num("valuenum"),
    cat("flag"),
    cat("valueuom")
],
options={
    'db_schema': 'mimic2v26'
})

d_demographicitems_schema = Schema("d_demographicitems", [
    id_("itemid"),
    cat("label"),
    cat("category")
],
options={
    'db_schema': 'mimic2v26'
})

d_parammap_items_schema = Schema("d_parammap_items", [
    cat("category"),
    cat("description")
],
options={
    'db_schema': 'mimic2v26'
})

d_codeditems_schema = Schema("d_codeditems", [
    id_("itemid"),
    cat("code"),
    cat("type"),
    cat("category"),
    cat("label"),
    cat("description")
],
options={
    'db_schema': 'mimic2v26'
})

d_careunits_schema = Schema("d_careunits", [
    id_("cuid"),
    cat("label")
],
options={
    'db_schema': 'mimic2v26'
})

comorbidity_scores_schema = Schema("comorbidity_scores", [
    num("subject_id"),
    num("hadm_id"),
    cat("category"),
    num("congestive_heart_failure"),
    num("cardiac_arrhythmias"),
    num("valvular_disease"),
    num("pulmonary_circulation"),
    num("peripheral_vascular"),
    num("hypertension"),
    num("paralysis"),
    num("other_neurological"),
    num("chronic_pulmonary"),
    num("diabetes_uncomplicated"),
    num("diabetes_complicated"),
    num("hypothyroidism"),
    num("renal_failure"),
    num("liver_disease"),
    num("peptic_ulcer"),
    num("aids"),
    num("lymphoma"),
    num("metastatic_cancer"),
    num("solid_tumor"),
    num("rheumatoid_arthritis"),
    num("coagulopathy"),
    num("obesity"),
    num("weight_loss"),
    num("fluid_electrolyte"),
    num("blood_loss_anemia"),
    num("deficiency_anemias"),
    num("alcohol_abuse"),
    num("drug_abuse"),
    num("psychoses"),
    num("depression")
],
options={
    'db_schema': 'mimic2v26'
})

d_ioitems_schema = Schema("d_ioitems", [
    id_("itemid"),
    cat("label"),
    cat("category")
],
options={
    'db_schema': 'mimic2v26'
})

icustay_detail_schema = Schema("icustay_detail", [
    num("icustay_id"),
    num("subject_id"),
    cat("gender"),
    big_dt("dob"),
    big_dt("dod"),
    cat("expire_flg"),
    num("subject_icustay_total_num"),
    num("subject_icustay_seq"),
    num("hadm_id"),
    num("hospital_total_num"),
    num("hospital_seq"),
    cat("hospital_first_flg"),
    cat("hospital_last_flg"),
    big_dt("hospital_admit_dt"),
    big_dt("hospital_disch_dt"),
    num("hospital_los"),
    cat("hospital_expire_flg"),
    num("icustay_total_num"),
    num("icustay_seq"),
    cat("icustay_first_flg"),
    cat("icustay_last_flg"),
    big_dt("icustay_intime"),
    big_dt("icustay_outtime"),
    num("icustay_admit_age"),
    cat("icustay_age_group"),
    num("icustay_los"),
    cat("icustay_expire_flg"),
    cat("icustay_first_careunit"),
    cat("icustay_last_careunit"),
    cat("icustay_first_service"),
    cat("icustay_last_service"),
    num("height"),
    num("weight_first"),
    num("weight_min"),
    num("weight_max"),
    num("sapsi_first"),
    num("sapsi_min"),
    num("sapsi_max"),
    num("sofa_first"),
    num("sofa_min"),
    num("sofa_max"),
    num("matched_waveforms_num")
],
options={
    'db_schema': 'mimic2v26'
})

demographic_detail_schema = Schema("demographic_detail", [
    num("subject_id"),
    num("hadm_id"),
    num("marital_status_itemid"),
    cat("marital_status_descr"),
    num("ethnicity_itemid"),
    cat("ethnicity_descr"),
    num("overall_payor_group_itemid"),
    cat("overall_payor_group_descr"),
    num("religion_itemid"),
    cat("religion_descr"),
    num("admission_type_itemid"),
    cat("admission_type_descr"),
    num("admission_source_itemid"),
    cat("admission_source_descr")
],
options={
    'db_schema': 'mimic2v26'
})

procedureevents_schema = Schema("procedureevents", [
    id_("subject_id"),
    id_("hadm_id"),
    id_("itemid"),
    id_("sequence_num"),
    big_dt("proc_dt")
],
options={
    'db_schema': 'mimic2v26'
})

chartevents_schema = Schema("chartevents", [
    id_("subject_id"),
    id_("icustay_id"),
    id_("itemid"),
    big_dt("charttime"),
    num("elemid"),
    big_dt("realtime"),
    id_("cgid"),
    id_("cuid"),
    cat("value1"),
    num("value1num"),
    cat("value1uom"),
    cat("value2"),
    num("value2num"),
    cat("value2uom"),
    cat("resultstatus"),
    cat("stopped")
],
options={
    'db_schema': 'mimic2v26'
})

d_patients_schema = Schema("d_patients", [
    id_("subject_id"),
    cat("sex"),
    big_dt("dob"),
    big_dt("dod"),
    cat("hospital_expire_flg")
],
options={
    'db_schema': 'mimic2v26'
})

poe_med_schema = Schema("poe_med", [
    id_("poe_id"),
    cat("drug_type"),
    cat("drug_name"),
    cat("drug_name_generic"),
    cat("prod_strength"),
    cat("form_rx"),
    cat("dose_val_rx"),
    cat("dose_unit_rx"),
    cat("form_val_disp"),
    cat("form_unit_disp"),
    num("dose_val_disp"),
    cat("dose_unit_disp"),
    cat("dose_range_override")
],
options={
    'db_schema': 'mimic2v26'
})

microbiologyevents_schema = Schema("microbiologyevents", [
    id_("subject_id"),
    id_("hadm_id"),
    big_dt("charttime"),
    id_("spec_itemid"),
    id_("org_itemid"),
    num("isolate_num"),
    id_("ab_itemid"),
    cat("dilution_amount"),
    cat("dilution_comparison"),
    cat("interpretation")
],
options={
    'db_schema': 'mimic2v26'
})

