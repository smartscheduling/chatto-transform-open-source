from chatto_transform.schema.schema_base import *

admissions_schema = Schema("admissions", [
    id_("row_id"),
    id_("subject_id"),
    num("hadm_id"),
    dt("admittime"),
    dt("dischtime"),
    dt("deathtime"),
    cat("admission_type"),
    cat("admission_location"),
    cat("discharge_location"),
    cat("insurance"),
    cat("language"),
    cat("religion"),
    cat("marital_status"),
    cat("ethnicity"),
    cat("diagnosis")
],
options={
    'db_schema': 'mimiciii'
})

callout_schema = Schema("callout", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    num("submit_wardid"),
    cat("submit_careunit"),
    num("curr_wardid"),
    cat("curr_careunit"),
    num("callout_wardid"),
    cat("callout_service"),
    num("request_tele"),
    num("request_resp"),
    num("request_cdiff"),
    num("request_mrsa"),
    num("request_vre"),
    cat("callout_status"),
    cat("callout_outcome"),
    num("discharge_wardid"),
    cat("acknowledge_status"),
    dt("createtime"),
    dt("updatetime"),
    dt("acknowledgetime"),
    dt("outcometime"),
    dt("firstreservationtime"),
    dt("currentreservationtime")
],
options={
    'db_schema': 'mimiciii'
})

caregivers_schema = Schema("caregivers", [
    id_("row_id"),
    num("cgid"),
    cat("label"),
    cat("description")
],
options={
    'db_schema': 'mimiciii'
})

chartevents_schema = Schema("chartevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    id_("itemid"),
    dt("charttime"),
    dt("storetime"),
    num("cgid"),
    cat("value"),
    num("valuenum"),
    cat("uom"),
    num("warning"),
    num("error"),
    cat("resultstatus"),
    cat("stopped")
],
options={
    'db_schema': 'mimiciii'
})

cptevents_schema = Schema("cptevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    cat("costcenter"),
    dt("chartdate"),
    cat("cpt_cd"),
    num("cpt_number"),
    cat("cpt_suffix"),
    num("ticket_id_seq"),
    cat("sectionheader"),
    cat("subsectionheader"),
    cat("description")
],
options={
    'db_schema': 'mimiciii'
})

d_cpt_schema = Schema("d_cpt", [
    id_("row_id"),
    num("category"),
    cat("sectionrange"),
    cat("sectionheader"),
    cat("subsectionrange"),
    cat("subsectionheader"),
    cat("codesuffix"),
    num("mincodeinsubsection"),
    num("maxcodeinsubsection")
],
options={
    'db_schema': 'mimiciii'
})

d_icd_diagnoses_schema = Schema("d_icd_diagnoses", [
    id_("row_id"),
    cat("icd9_code"),
    cat("short_title"),
    cat("long_title")
],
options={
    'db_schema': 'mimiciii'
})

d_icd_procedures_schema = Schema("d_icd_procedures", [
    id_("row_id"),
    cat("icd9_code"),
    cat("short_title"),
    cat("long_title")
],
options={
    'db_schema': 'mimiciii'
})

d_items_schema = Schema("d_items", [
    id_("row_id"),
    num("itemid"),
    cat("label"),
    cat("abbreviation"),
    cat("dbsource"),
    cat("linksto"),
    cat("code"),
    cat("category"),
    cat("unitname"),
    cat("param_type"),
    num("lownormalvalue"),
    num("highnormalvalue")
],
options={
    'db_schema': 'mimiciii'
})

d_labitems_schema = Schema("d_labitems", [
    id_("row_id"),
    num("itemid"),
    cat("label"),
    cat("fluid"),
    cat("category"),
    cat("loinc_code")
],
options={
    'db_schema': 'mimiciii'
})

datetimeevents_schema = Schema("datetimeevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    id_("itemid"),
    dt("charttime"),
    dt("storetime"),
    num("cgid"),
    dt("value"),
    cat("uom"),
    num("warning"),
    num("error"),
    cat("resultstatus"),
    cat("stopped")
],
options={
    'db_schema': 'mimiciii'
})

diagnoses_icd_schema = Schema("diagnoses_icd", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    num("sequence"),
    cat("icd9_code"),
    cat("description")
],
options={
    'db_schema': 'mimiciii'
})

drgcodes_schema = Schema("drgcodes", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    cat("drg_type"),
    cat("drg_code"),
    cat("description"),
    num("drg_severity"),
    num("drg_mortality")
],
options={
    'db_schema': 'mimiciii'
})

icustayevents_schema = Schema("icustayevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    num("icustay_id"),
    cat("dbsource"),
    cat("first_careunit"),
    cat("last_careunit"),
    num("first_wardid"),
    num("last_wardid"),
    dt("intime"),
    dt("outtime"),
    num("los")
],
options={
    'db_schema': 'mimiciii'
})

ioevents_schema = Schema("ioevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    dt("starttime"),
    dt("endtime"),
    num("itemid"),
    num("volume"),
    cat("volumeuom"),
    num("rate"),
    cat("rateuom"),
    dt("storetime"),
    num("cgid"),
    num("orderid"),
    num("linkorderid"),
    cat("ordercategoryname"),
    cat("secondaryordercategoryname"),
    cat("ordercomponenttypedescription"),
    cat("ordercategorydescription"),
    num("patientweight"),
    num("totalvolume"),
    cat("totalvolumeuom"),
    cat("statusdescription"),
    cat("stopped"),
    num("newbottle"),
    num("isopenbag"),
    num("continueinnextdept"),
    num("cancelreason"),
    cat("comments_status"),
    cat("comments_title"),
    dt("comments_date"),
    dt("originalcharttime"),
    num("originalamount"),
    cat("originalamountuom"),
    cat("originalroute"),
    num("originalrate"),
    cat("originalrateuom"),
    cat("originalsite")
],
options={
    'db_schema': 'mimiciii'
})

labevents_schema = Schema("labevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("itemid"),
    dt("charttime"),
    cat("value"),
    num("valuenum"),
    cat("uom"),
    cat("flag")
],
options={
    'db_schema': 'mimiciii'
})

microbiologyevents_schema = Schema("microbiologyevents", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    dt("chartdate"),
    dt("charttime"),
    num("spec_itemid"),
    cat("spec_type_cd"),
    cat("spec_type_desc"),
    num("org_itemid"),
    num("org_cd"),
    cat("org_name"),
    num("isolate_num"),
    num("ab_itemid"),
    num("ab_cd"),
    cat("ab_name"),
    cat("dilution_text"),
    cat("dilution_comparison"),
    num("dilution_value"),
    cat("interpretation")
],
options={
    'db_schema': 'mimiciii'
})

noteevents_schema = Schema("noteevents", [
    id_("row_id"),
    num("record_id"),
    id_("subject_id"),
    id_("hadm_id"),
    dt("chartdate"),
    cat("category"),
    cat("description"),
    id_("cgid"),
    cat("iserror"),
    cat("text")
],
options={
    'db_schema': 'mimiciii'
})

patients_schema = Schema("patients", [
    id_("row_id"),
    num("subject_id"),
    cat("gender"),
    dt("dob"),
    dt("dod"),
    dt("dod_hosp"),
    dt("dod_ssn"),
    cat("hospital_expire_flag")
],
options={
    'db_schema': 'mimiciii'
})

prescriptions_schema = Schema("prescriptions", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    dt("starttime"),
    dt("endtime"),
    cat("drug_type"),
    cat("drug"),
    cat("drug_name_poe"),
    cat("drug_name_generic"),
    cat("formulary_drug_cd"),
    cat("gsn"),
    cat("ndc"),
    cat("prod_strength"),
    cat("dose_val_rx"),
    cat("dose_unit_rx"),
    cat("form_val_disp"),
    cat("form_unit_disp"),
    cat("route")
],
options={
    'db_schema': 'mimiciii'
})

procedures_icd_schema = Schema("procedures_icd", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    num("proc_seq_num"),
    cat("icd9_code")
],
options={
    'db_schema': 'mimiciii'
})

services_schema = Schema("services", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    dt("transfertime"),
    cat("prev_service"),
    cat("curr_service")
],
options={
    'db_schema': 'mimiciii'
})

transfers_schema = Schema("transfers", [
    id_("row_id"),
    id_("subject_id"),
    id_("hadm_id"),
    id_("icustay_id"),
    cat("dbsource"),
    cat("eventtype"),
    cat("prev_careunit"),
    cat("curr_careunit"),
    num("prev_wardid"),
    num("curr_wardid"),
    dt("intime"),
    dt("outtime"),
    num("los")
],
options={
    'db_schema': 'mimiciii'
})

