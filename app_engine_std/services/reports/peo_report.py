#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Note:
#   Python code was exported from the PEO-Report jupyter notebook in the 'ops-data-service' github project,
#   with minor changes to run in the cloud.
#
import datetime as dt
import pandas as pd
import warnings

from functools import reduce

warnings.filterwarnings('ignore')


def generate_peo_report(db_postgres):
    """
    Generate the PEO Report
    """
    # Set some parameters (note: All the timestamps stored in the database are in the UTC timezone)
    today = dt.datetime.utcnow()
    yday = (today - dt.timedelta(days=1)).strftime('%Y-%m-%d')  # based on the previous day’s data

    if today.weekday() == 0:  # If today is Monday, since_last_report_date is last Thursday's date, since the previous run was Friday
        sinceLast_rp_d = (today - dt.timedelta(days=4)).strftime('%Y-%m-%d')
    else:  # otherwise since_last_report_date is the day before yesterday
        sinceLast_rp_d = (today - dt.timedelta(days=2)).strftime('%Y-%m-%d')

    if today.weekday() == 0:  # If today is Monday, subtract a week to prev Monday
        prev_monday = (today - dt.timedelta(days=7)).strftime('%Y-%m-%d')
    else:  # otherwise subtract today.weekday() days to get to Monday
        prev_monday = (today - dt.timedelta(days=today.weekday())).strftime('%Y-%m-%d')

    query = f'''

    SELECT DISTINCT p.participant_id, 
           CASE WHEN s.hpo_lite_flag = 1 AND h.organization_type = 'HPO' THEN 'HPO-Lite' 
                ELSE h.organization_type
           END AS organization_type, 
           p.awardee_name,
           p.organization_name, 
           p.site_name,
           CASE WHEN DATE(p.sign_up_time) <= CAST('{yday}' AS Date) THEN 1 ELSE 0 
                END AS total_registered_individuals,
           CASE WHEN p.primary_consent_date IS NOT NULL AND p.primary_consent_date <= CAST('{yday}' AS Date) THEN 1 ELSE 0 
                END AS total_participants_consented,
           CASE WHEN p.enrl_core_participant_time IS NOT NULL AND DATE(p.enrl_core_participant_time) <= CAST('{yday}' AS Date)
                THEN 'CORE_PARTICIPANT'
                WHEN p.enrl_core_participant_minus_pm_time IS NOT NULL AND DATE(p.enrl_core_participant_minus_pm_time) <= CAST('{yday}' AS Date)
                THEN 'CORE_MINUS_PM'
                WHEN p.enrl_participant_plus_ehr_time_current IS NOT NULL AND DATE(p.enrl_participant_plus_ehr_time_current) <= CAST('{yday}' AS Date)
                THEN 'FULLY_CONSENTED'
                WHEN p.enrl_participant_time IS NOT NULL AND DATE(p.enrl_participant_time) <= CAST('{yday}' AS Date)
                THEN 'PARTICIPANT'
                WHEN DATE(p.sign_up_time) <= CAST('{yday}' AS Date)
                THEN 'REGISTERED'
                END AS enrollment_status,
           CASE WHEN p.enrl_core_participant_time IS NOT NULL AND DATE(p.enrl_core_participant_time) <= CAST('{sinceLast_rp_d}' AS Date)
                THEN 'CORE_PARTICIPANT'
                WHEN p.enrl_core_participant_minus_pm_time IS NOT NULL AND DATE(p.enrl_core_participant_minus_pm_time) <= CAST('{sinceLast_rp_d}' AS Date)
                THEN 'CORE_MINUS_PM'
                WHEN p.enrl_participant_plus_ehr_time_current IS NOT NULL AND DATE(p.enrl_participant_plus_ehr_time_current) <= CAST('{sinceLast_rp_d}' AS Date)
                THEN 'FULLY_CONSENTED'
                WHEN p.enrl_participant_time IS NOT NULL AND DATE(p.enrl_participant_time) <= CAST('{sinceLast_rp_d}' AS Date)
                THEN 'PARTICIPANT'
                WHEN DATE(p.sign_up_time) <= CAST('{sinceLast_rp_d}' AS Date)
                THEN 'REGISTERED'
                END AS enrollment_status_since_last_report,
           CASE WHEN p.enrl_core_participant_time IS NOT NULL AND DATE(p.enrl_core_participant_time) <= CAST('{prev_monday}' AS Date)
                THEN 'CORE_PARTICIPANT'
                WHEN p.enrl_core_participant_minus_pm_time IS NOT NULL AND DATE(p.enrl_core_participant_minus_pm_time) <= CAST('{prev_monday}' AS Date)
                THEN 'CORE_MINUS_PM'
                END AS weekly_tally,
           p.ubr_overall, 
           p.ubr_ethnicity, 
           p.ubr_age_at_consent, 
           p.ubr_sex, 
           p.ubr_sexual_gender_minority,
           p.ubr_income, 
           p.ubr_education, 
           p.ubr_geography,
           p.ubr_disability

    FROM pdr.mv_participant_display p
    LEFT JOIN pdr.mv_hpo h USING(hpo_id)
    FULL JOIN pdr.v_site_all s USING(hpo_id)

    '''
    data = pd.read_sql(query, db_postgres)
    data.head()

    # ---------------------------------------------------------------
    ## Participant Enrollment Overview (PEO) - Tab 1
    # ---------------------------------------------------------------
    ### Enrollment Status (non-overlapping)
    # - REGISTERED = 0
    # - PARTICIPANT = 1
    # - FULLY_CONSENTED = 2
    # - CORE_PARTICIPANT = 3
    # - CORE_MINUS_PM = 4
    # ---------------------------------------------------------------
    
    Total_Registered = data.groupby(['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Total Registered'}).sort_values(['organization_type', 'awardee_name'])
    Total_Participants_Consented = \
    data.loc[(data['total_participants_consented'] == 1)].groupby(['organization_type', 'awardee_name'])[
        'participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Total Participants (Consented)'}).sort_values(['organization_type', 'awardee_name'])
    
    total_df = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                      [Total_Registered, Total_Participants_Consented])
    
    for c in total_df.columns:
        total_df = total_df.rename(columns={c: ('', c)})
    
    total_df.columns = pd.MultiIndex.from_tuples(tuple(total_df.columns))
    
    total_df.loc['Overall', :] = total_df.sum().values
    total_df = total_df.rename(index={'': 'All Awardees'})
    total_df = total_df.reindex(['Overall', 'HPO', 'FQHC', 'DV', 'VA', 'HPO-Lite', 'UNSET'], level='organization_type')
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    # enrollment from yesterday
    enrollment_df = data.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'],
                                     columns='enrollment_status', aggfunc='nunique').reset_index()
    enrollment_df = enrollment_df[
        ['organization_type', 'awardee_name', 'REGISTERED', 'PARTICIPANT', 'FULLY_CONSENTED', 'CORE_MINUS_PM',
         'CORE_PARTICIPANT']]
    enrollment_df = enrollment_df.rename(
        columns={'REGISTERED': 'Registered Individuals', 'PARTICIPANT': 'Enrolled Participants',
                 'FULLY_CONSENTED': 'Participant +EHR', 'CORE_MINUS_PM': 'Total Core -PM',
                 'CORE_PARTICIPANT': 'Total Core Participant'}).set_index(['organization_type', 'awardee_name'])
    
    # enrollment since previous report run
    enroll_sinceLast_rp_df = data.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'],
                                              columns='enrollment_status_since_last_report',
                                              aggfunc='nunique').reset_index()
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df[
        ['organization_type', 'awardee_name', 'REGISTERED', 'PARTICIPANT', 'FULLY_CONSENTED', 'CORE_MINUS_PM',
         'CORE_PARTICIPANT']]
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df.rename(
        columns={'REGISTERED': 'Registered_prev', 'PARTICIPANT': 'Participant_prev',
                 'FULLY_CONSENTED': 'Participant +EHR_prev', 'CORE_MINUS_PM': 'Core -PM_prev',
                 'CORE_PARTICIPANT': 'Core Participant_prev'}).set_index(['organization_type', 'awardee_name'])
    
    # weekly tally
    weekly_tally_df = data.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'],
                                       columns='weekly_tally', aggfunc='nunique')
    weekly_tally_df = weekly_tally_df.rename(columns={'CORE_MINUS_PM': 'CPM_prev_week', 'CORE_PARTICIPANT': 'CP_prev_week'})
    
    # merge
    enroll_full_df = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                            [enrollment_df, enroll_sinceLast_rp_df, weekly_tally_df])
    enroll_full_df.loc['Overall', :] = enroll_full_df.sum().values
    enroll_full_df = enroll_full_df.rename(index={'': 'All Awardees'})
    enroll_full_df = enroll_full_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # Date cols
    enroll_full_df['Registered Since Last Report'] = enroll_full_df['Registered Individuals'] - enroll_full_df[
        'Registered_prev']
    enroll_full_df['Participant Since Last Report'] = enroll_full_df['Enrolled Participants'] - enroll_full_df[
        'Participant_prev']
    enroll_full_df['Participant +EHR Since Last Report'] = enroll_full_df['Participant +EHR'] - enroll_full_df[
        'Participant +EHR_prev']
    enroll_full_df['Since Last Report'] = enroll_full_df['Total Core -PM'] - enroll_full_df['Core -PM_prev']
    enroll_full_df['CP Since Last Report'] = enroll_full_df['Total Core Participant'] - enroll_full_df[
        'Core Participant_prev']
    enroll_full_df['Weekly tally'] = enroll_full_df['Total Core -PM'] - enroll_full_df['CPM_prev_week']
    enroll_full_df['CP Weekly Tally'] = enroll_full_df['Total Core Participant'] - enroll_full_df['CP_prev_week']
    enroll_full_df = enroll_full_df.drop(
        ['Registered_prev', 'Participant_prev', 'Participant +EHR_prev', 'Core -PM_prev', 'Core Participant_prev',
         'CPM_prev_week', 'CP_prev_week'], axis=1)
    enroll_full_df = enroll_full_df[
        ['Registered Individuals', 'Registered Since Last Report', 'Enrolled Participants', 'Participant Since Last Report',
         'Participant +EHR', 'Participant +EHR Since Last Report', 'Total Core -PM', 'Since Last Report', 'Weekly tally',
         'Total Core Participant', 'CP Since Last Report', 'CP Weekly Tally']]
    
    for c in enroll_full_df.columns:
        enroll_full_df = enroll_full_df.rename(columns={c: ('Enrollment Status (non-overlapping)', c)})
    
    enroll_full_df.columns = pd.MultiIndex.from_tuples(tuple(enroll_full_df.columns))
    
    # ---------------------------------------------------------------
    ###  UBR of Core Participants
    # - Overall
    # - Racial
    # Identity
    # - Age
    # - Sex
    # - SGM
    # - Income
    # - Education
    # - Geography
    # ---------------------------------------------------------------
    ubr_overall_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Overall'}).sort_values(['organization_type', 'awardee_name'])
    ubr_ethnicity_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Racial Identity'}).sort_values(['organization_type', 'awardee_name'])
    ubr_age_at_consent_c = \
    data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Age'}).sort_values(['organization_type', 'awardee_name'])
    ubr_sex_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Sex'}).sort_values(['organization_type', 'awardee_name'])
    ubr_sexual_gender_minority_c = \
    data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'SGM'}).sort_values(['organization_type', 'awardee_name'])
    ubr_income_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Income'}).sort_values(['organization_type', 'awardee_name'])
    ubr_education_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Education'}).sort_values(['organization_type', 'awardee_name'])
    ubr_geography_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Geography'}).sort_values(['organization_type', 'awardee_name'])
    ubr_disability_c = data.loc[(data['enrollment_status'] == 'CORE_PARTICIPANT') & (data['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Disability'}).sort_values(['organization_type', 'awardee_name'])
    
    # ---------------------------------------------------------------
    ubr_core_df = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                         [Total_Registered, ubr_overall_c, ubr_ethnicity_c, ubr_age_at_consent_c, ubr_sex_c,
                          ubr_sexual_gender_minority_c, ubr_income_c, ubr_education_c, ubr_geography_c, ubr_disability_c])
    ubr_core_df = ubr_core_df.drop(['Total Registered'], axis=1)
    ubr_core_df.loc['Overall', :] = ubr_core_df.sum().values
    ubr_core_df = ubr_core_df.rename(index={'': 'All Awardees'})
    
    ubr_core_df = ubr_core_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core Participant')].astype(float), axis=0)
    ubr_core_df = ubr_core_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_df.columns:
        ubr_core_df = ubr_core_df.rename(columns={c: ('UBR of Core Participants', c)})
    
    ubr_core_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_df.columns))
    
    # ---------------------------------------------------------------
    ### UBR of Core -PM Participants
    # - Overall
    # - Racial
    # Identity
    # - Age
    # - Sex
    # - SGM
    # - Income
    # - Education
    # - Geography
    # ---------------------------------------------------------------
    ubr_overall_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Overall'}).sort_values(['organization_type', 'awardee_name'])
    ubr_ethnicity_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Racial Identity'}).sort_values(['organization_type', 'awardee_name'])
    ubr_age_at_consent_cpm = \
    data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Age'}).sort_values(['organization_type', 'awardee_name'])
    ubr_sex_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Sex'}).sort_values(['organization_type', 'awardee_name'])
    ubr_sexual_gender_minority_cpm = \
    data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'SGM'}).sort_values(['organization_type', 'awardee_name'])
    ubr_income_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Income'}).sort_values(['organization_type', 'awardee_name'])
    ubr_education_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Education'}).sort_values(['organization_type', 'awardee_name'])
    ubr_geography_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Geography'}).sort_values(['organization_type', 'awardee_name'])
    ubr_disability_cpm = data.loc[(data['enrollment_status'] == 'CORE_MINUS_PM') & (data['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Disability'}).sort_values(['organization_type', 'awardee_name'])
    
    # ---------------------------------------------------------------
    ubr_core_minus_pm_df = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                                  [Total_Registered, ubr_overall_cpm, ubr_ethnicity_cpm, ubr_age_at_consent_cpm,
                                   ubr_sex_cpm, ubr_sexual_gender_minority_cpm, ubr_income_cpm, ubr_education_cpm,
                                   ubr_geography_cpm, ubr_disability_cpm])
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.drop(['Total Registered'], axis=1)
    ubr_core_minus_pm_df.loc['Overall', :] = ubr_core_minus_pm_df.sum().values
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.rename(index={'': 'All Awardees'})
    
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core -PM')].astype(float), axis=0)
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_minus_pm_df.columns:
        ubr_core_minus_pm_df = ubr_core_minus_pm_df.rename(columns={c: ('UBR of Core -PM Participants', c)})
    
    ubr_core_minus_pm_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_minus_pm_df.columns))
    
    # ---------------------------------------------------------------
    # Both UBR metrics
    ubr_dfs = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                     [ubr_core_df, ubr_core_minus_pm_df])
    
    # ---------------------------------------------------------------
    ###  Gender Identity/Racial Identity/Age
    # ---------------------------------------------------------------
    query = f"""
    SELECT DISTINCT
        p.participant_id,
        CASE
            WHEN s.hpo_lite_flag = 1 AND h.organization_type = 'HPO' THEN 'HPO-Lite'
            ELSE h.organization_type
        END AS organization_type,
        p.awardee_name,
        p.organization_name,
        p.site_name,
        CASE
            WHEN gen.gender_genderidentity = 'GenderIdentity_Man' THEN 'Man'
            WHEN gen.gender_genderidentity = 'GenderIdentity_NonBinary' THEN 'Non-Binary'
            WHEN gen.gender_genderidentity = 'GenderIdentity_Transgender' THEN 'Transgender'
            WHEN gen.gender_genderidentity = 'GenderIdentity_AdditionalOptions' THEN 'Other/Addl. Options'
            WHEN gen.gender_genderidentity = 'GenderIdentity_Woman' THEN 'Woman'
            WHEN gen.gender_genderidentity = 'PMI_PreferNotToAnswer' OR
                 gen.gender_genderidentity = 'GenderIdentity_PreferNotToAnswer' THEN 'Prefer not to say'
            WHEN gen.gender_genderidentity = 'PMI_Skip' THEN 'Skipped'
            WHEN gen.gender_genderidentity IS NULL THEN NULL
            ELSE 'Multiple Selections'
        END AS gender_identity,
        CASE
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_Hispanic,WhatRaceEthnicity_White' OR
                 race.race_whatraceethnicity = 'WhatRaceEthnicity_White,WhatRaceEthnicity_Hispanic'
                THEN 'White and Hispanic, Latino, or Spanish'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_Asian' THEN 'Asian'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_Hispanic,WhatRaceEthnicity_Black' OR
                 race.race_whatraceethnicity = 'WhatRaceEthnicity_Black,WhatRaceEthnicity_Hispanic'
                THEN 'Black and Hispanic, Latino, or Spanish'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_Black' THEN 'Black or African American'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_MENA' THEN 'Middle Eastern or North African'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_NHPI'
                THEN 'Native Hawaiian or other Pacific Islander'
            WHEN race.race_whatraceethnicity LIKE '%%WhatRaceEthnicity_Hispanic%%' AND
                 (LENGTH(race.race_whatraceethnicity) - LENGTH(REPLACE(race.race_whatraceethnicity, ',', '')) >=
                  2) THEN 'More than one race and Hispanic, Latino, or Spanish'
            WHEN race.race_whatraceethnicity LIKE '%%WhatRaceEthnicity_Hispanic%%' AND
                 (LENGTH(race.race_whatraceethnicity) - LENGTH(REPLACE(race.race_whatraceethnicity, ',', '')) =
                  1) THEN 'One other race and Hispanic, Latino, or Spanish'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_Hispanic' THEN 'Hispanic, Latino, or Spanish'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_AIAN' OR
                 race.race_whatraceethnicity = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese' THEN 'Other race'
            WHEN race.race_whatraceethnicity = 'PMI_PreferNotToAnswer' THEN 'Prefer not to say'
            WHEN race.race_whatraceethnicity = 'WhatRaceEthnicity_White' THEN 'White'
            WHEN race.race_whatraceethnicity = 'PMI_Skip' THEN 'Skipped'
            WHEN LENGTH(race.race_whatraceethnicity) - LENGTH(REPLACE(race.race_whatraceethnicity, ',', '')) >=
                 1 THEN 'More than one race'
        END AS race_ethnicity,
        CASE
            WHEN mvp.current_age < 18 THEN '0-17'
            WHEN mvp.current_age < 26 THEN '18-25'
            WHEN mvp.current_age < 36 THEN '26-35'
            WHEN mvp.current_age < 46 THEN '36-45'
            WHEN mvp.current_age < 56 THEN '46-55'
            WHEN mvp.current_age < 66 THEN '56-65'
            WHEN mvp.current_age < 76 THEN '66-75'
            WHEN mvp.current_age < 86 THEN '76-85'
            WHEN mvp.current_age >= 86 THEN '86 and over'
        END AS age_group        
        FROM pdr.mv_participant_display p
         LEFT JOIN pdr.mv_hpo h USING (hpo_id)
         FULL JOIN pdr.v_site_all s USING (hpo_id)
        
         JOIN (SELECT participant_id,
                      EXTRACT(YEAR FROM age(cast(date_of_birth as date))) AS current_age
               FROM pdr.mv_participant_all) mvp ON p.participant_id = mvp.participant_id
        
         LEFT OUTER JOIN (
            --- Gender Identity
            SELECT participant_id, gender_genderidentity
            FROM (
                 SELECT participant_id, gender_genderidentity,
                      row_number() over (partition by participant_id order by authored ASC) as rn
                 FROM pdr.mv_mod_thebasics_first_resp tb
                 WHERE gender_genderidentity IS NOT NULL
                 ) a
            WHERE rn = 1) gen ON p.participant_id = gen.participant_id
        
         LEFT OUTER JOIN (
        --- Race / Ethnicity
            SELECT participant_id, race_whatraceethnicity
            FROM (
                 SELECT participant_id, race_whatraceethnicity,
                      row_number() over (partition by participant_id order by authored ASC) as rn
                 FROM pdr.mv_mod_thebasics_first_resp tb
                 WHERE race_whatraceethnicity IS NOT NULL
                 ) a
            WHERE rn = 1) race ON p.participant_id = race.participant_id
        WHERE DATE(p.sign_up_time) <= CAST('{yday}' AS Date)
    """
    
    demog = pd.read_sql(query, db_postgres)
    demog.head()
    
    # ---------------------------------------------------------------
    gender_df = demog.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'],
                                  columns='gender_identity', aggfunc='nunique').reset_index()
    race_df = demog.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'],
                                columns='race_ethnicity', aggfunc='nunique').reset_index()
    age_df = demog.pivot_table(values='participant_id', index=['organization_type', 'awardee_name'], columns='age_group',
                               aggfunc='nunique').reset_index()

    # ---------------------------------------------------------------
    # Gender
    gender_df = gender_df[
        ['organization_type', 'awardee_name', 'Man', 'Non-Binary', 'Other/Addl. Options', 'Transgender', 'Woman',
         'Multiple Selections', 'Skipped', 'Prefer not to say']]
    gender_df = gender_df.set_index(['organization_type', 'awardee_name'])
    
    for c in gender_df.columns:
        gender_df = gender_df.rename(columns={c: ('Gender Identity', c)})
    
    gender_df.columns = pd.MultiIndex.from_tuples(tuple(gender_df.columns))
    
    gender_df.loc['Overall', :] = gender_df.sum().values
    gender_df = gender_df.rename(index={'': 'All Awardees'})
    
    # Race
    race_df = race_df[['organization_type', 'awardee_name', 'Asian', 'Black and Hispanic, Latino, or Spanish',
                       'Black or African American', 'Hispanic, Latino, or Spanish', 'Middle Eastern or North African',
                       'More than one race', 'More than one race and Hispanic, Latino, or Spanish',
                       'Native Hawaiian or other Pacific Islander', 'One other race and Hispanic, Latino, or Spanish',
                       'Other race', 'Prefer not to say', 'White', 'White and Hispanic, Latino, or Spanish', 'Skipped']]
    race_df = race_df.set_index(['organization_type', 'awardee_name'])
    
    for c in race_df.columns:
        race_df = race_df.rename(columns={c: ('Racial Identity', c)})
    
    race_df.columns = pd.MultiIndex.from_tuples(tuple(race_df.columns))
    
    race_df.loc['Overall', :] = race_df.sum().values
    race_df = race_df.rename(index={'': 'All Awardees'})
    
    # Age
    age_df = age_df.set_index(['organization_type', 'awardee_name'])
    
    for c in age_df.columns:
        age_df = age_df.rename(columns={c: ('Age', c)})
    
    age_df.columns = pd.MultiIndex.from_tuples(tuple(age_df.columns))
    
    age_df.loc['Overall', :] = age_df.sum().values
    age_df = age_df.rename(index={'': 'All Awardees'})
    
    # ---------------------------------------------------------------
    # All Demographics
    demog_df = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                      [gender_df, race_df, age_df])
    demog_df = demog_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    ### Combined dfs
    # ---------------------------------------------------------------
    all_dfs_tab1 = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name'], how='left'),
                          [total_df, enroll_full_df, ubr_dfs, demog_df])
    
    all_dfs_tab1.index = all_dfs_tab1.index.rename(['Type', 'Awardee'])
    
    all_dfs_tab1 = all_dfs_tab1.rename(index={'HPO': 'RMC', 'UNSET': 'UNPAIRED', 'No organization set': 'Unpaired',
                                              'United States Department of Veteran Affairs': 'VA',
                                              'Cherokee Health Systems': 'Cherokee',
                                              'Community Health Center, Inc': 'Community Health Center',
                                              'Eau Claire Cooperative Health Center': 'Eau Claire',
                                              'Hudson River Health Care, Inc.': 'HRHCare',
                                              'Jackson-Hinds Comprehensive Health Center': 'Jackson-Hinds',
                                              'San Ysidro Health Center': 'San Ysidro',
                                              'California Precision Medicine Consortium': 'California',
                                              'New England Precision Medicine Consortium': 'New England',
                                              'Pittsburgh': 'PITT', 'Southern Consortium': 'Southern',
                                              'Trans-American Consortium for the Health Care Systems Research Network (TACH)': 'Trans-America',
                                              'University of Texas Health Science Center at Houston': 'UT_HEALTH',
                                              'Virginia Commonwealth University': 'VCU',
                                              'Washington University in St. Louis': 'WASH U',
                                              'Wisconsin Consortium': 'Wisconsin', 'Quest Labs': 'Quest'})
    
    # ---------------------------------------------------------------
    # Move ubr_disability to the end
    ubr_disabilities = all_dfs_tab1.iloc[:, all_dfs_tab1.columns.get_level_values(1) == 'Disability']
    tab1_final = all_dfs_tab1.drop(
        [('UBR of Core Participants', 'Disability'), ('UBR of Core -PM Participants', 'Disability')], axis=1)
    tab1_final = reduce(lambda x, y: pd.merge(x, y, on=['Type', 'Awardee'], how='left'), [tab1_final, ubr_disabilities])
    tab1_final = tab1_final.rename(
        columns={'Registered Since Last Report': 'Since Last Report', 'Participant Since Last Report': 'Since Last Report',
                 'Participant +EHR Since Last Report': 'Since Last Report'})
    tab1_final.index = tab1_final.index.rename(['Type', 'Awardee / Organization / Sites'])
    
    # Extra columns from Scott's report
    tab1_final.insert(loc=9, column=('Enrollment Status (non-overlapping)', 'Since Reactivation'), value='-')
    tab1_final.insert(loc=13, column=('Enrollment Status (non-overlapping)', 'CP Since Reactivation'), value='-')
    tab1_final.insert(loc=16, column=(' ', '# Days Since Reactivation'), value='-')
    
    # no data 0-17 age bucket
    tab1_final.insert(loc=55, column=('Age', '0-17'), value=0)
    
    # ---------------------------------------------------------------
    ## Participant Enrollment Overview (PEO) - Tab 2 (Including Orgs & Sites)
    # ---------------------------------------------------------------
    ### Organizations
    # ---------------------------------------------------------------
    #### Enrollment Status (non-overlapping)
    # ---------------------------------------------------------------
    data_org = data.copy()
    data_org['organization_name'] = data_org['organization_name'].fillna('zUnpaired')
    
    # ---------------------------------------------------------------
    Total_Registered = data_org.groupby(['organization_type', 'awardee_name', 'organization_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Total Registered'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name']).astype(float)
    Total_Participants_Consented = data_org.loc[(data_org['total_participants_consented'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Total Participants (Consented)'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name']).astype(float)
    
    total_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [Total_Registered, Total_Participants_Consented])
    
    for c in total_df.columns:
        total_df = total_df.rename(columns={c: ('', c)})
    
    total_df.columns = pd.MultiIndex.from_tuples(tuple(total_df.columns))
    
    total_df = total_df.reindex(['Overall', 'HPO', 'FQHC', 'DV', 'VA', 'HPO-Lite', 'UNSET'], level='organization_type')
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    # enrollment from yesterday
    enrollment_df = data_org.pivot_table(values='participant_id',
                                         index=['organization_type', 'awardee_name', 'organization_name'],
                                         columns='enrollment_status', aggfunc='nunique').reset_index()
    enrollment_df = enrollment_df[
        ['organization_type', 'awardee_name', 'organization_name', 'REGISTERED', 'PARTICIPANT', 'FULLY_CONSENTED',
         'CORE_MINUS_PM', 'CORE_PARTICIPANT']]
    enrollment_df = enrollment_df.rename(
        columns={'REGISTERED': 'Registered Individuals', 'PARTICIPANT': 'Enrolled Participants',
                 'FULLY_CONSENTED': 'Participant +EHR', 'CORE_MINUS_PM': 'Total Core -PM',
                 'CORE_PARTICIPANT': 'Total Core Participant'}).set_index(
        ['organization_type', 'awardee_name', 'organization_name'])
    
    # enrollment since previous report run
    enroll_sinceLast_rp_df = data_org.pivot_table(values='participant_id',
                                                  index=['organization_type', 'awardee_name', 'organization_name'],
                                                  columns='enrollment_status_since_last_report',
                                                  aggfunc='nunique').reset_index()
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df[
        ['organization_type', 'awardee_name', 'organization_name', 'REGISTERED', 'PARTICIPANT', 'FULLY_CONSENTED',
         'CORE_MINUS_PM', 'CORE_PARTICIPANT']]
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df.rename(
        columns={'REGISTERED': 'Registered_prev', 'PARTICIPANT': 'Participant_prev',
                 'FULLY_CONSENTED': 'Participant +EHR_prev', 'CORE_MINUS_PM': 'Core -PM_prev',
                 'CORE_PARTICIPANT': 'Core Participant_prev'}).set_index(
        ['organization_type', 'awardee_name', 'organization_name'])
    
    # weekly tally
    weekly_tally_df = data_org.pivot_table(values='participant_id',
                                           index=['organization_type', 'awardee_name', 'organization_name'],
                                           columns='weekly_tally', aggfunc='nunique')
    weekly_tally_df = weekly_tally_df.rename(columns={'CORE_MINUS_PM': 'CPM_prev_week', 'CORE_PARTICIPANT': 'CP_prev_week'})
    
    # merge
    enroll_full_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [enrollment_df, enroll_sinceLast_rp_df, weekly_tally_df])
    enroll_full_df.loc['Overall', :] = enroll_full_df.sum().values
    enroll_full_df = enroll_full_df.rename(index={'': 'All Awardees'})
    enroll_full_df = enroll_full_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # Date cols
    enroll_full_df['Registered Since Last Report'] = enroll_full_df['Registered Individuals'] - enroll_full_df[
        'Registered_prev']
    enroll_full_df['Participant Since Last Report'] = enroll_full_df['Enrolled Participants'] - enroll_full_df[
        'Participant_prev']
    enroll_full_df['Participant +EHR Since Last Report'] = enroll_full_df['Participant +EHR'] - enroll_full_df[
        'Participant +EHR_prev']
    enroll_full_df['Since Last Report'] = enroll_full_df['Total Core -PM'] - enroll_full_df['Core -PM_prev']
    enroll_full_df['CP Since Last Report'] = enroll_full_df['Total Core Participant'] - enroll_full_df[
        'Core Participant_prev']
    enroll_full_df['Weekly tally'] = enroll_full_df['Total Core -PM'] - enroll_full_df['CPM_prev_week']
    enroll_full_df['CP Weekly Tally'] = enroll_full_df['Total Core Participant'] - enroll_full_df['CP_prev_week']
    enroll_full_df = enroll_full_df.drop(
        ['Registered_prev', 'Participant_prev', 'Participant +EHR_prev', 'Core -PM_prev', 'Core Participant_prev',
         'CPM_prev_week', 'CP_prev_week'], axis=1)
    enroll_full_df = enroll_full_df[
        ['Registered Individuals', 'Registered Since Last Report', 'Enrolled Participants', 'Participant Since Last Report',
         'Participant +EHR', 'Participant +EHR Since Last Report', 'Total Core -PM', 'Since Last Report',
         'Weekly tally', 'Total Core Participant', 'CP Since Last Report', 'CP Weekly Tally']]
    
    for c in enroll_full_df.columns:
        enroll_full_df = enroll_full_df.rename(columns={c: ('Enrollment Status (non-overlapping)', c)})
    
    enroll_full_df.columns = pd.MultiIndex.from_tuples(tuple(enroll_full_df.columns))
    
    # ---------------------------------------------------------------
    #### UBR of Core Participants
    # ---------------------------------------------------------------
    ubr_overall_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Overall'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_ethnicity_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Racial Identity'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name'])
    ubr_age_at_consent_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Age'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_sex_c = data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Sex'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_sexual_gender_minority_c = data_org.loc[
        (data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'SGM'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_income_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Income'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_education_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Education'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_geography_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Geography'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_disability_c = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_PARTICIPANT') & (data_org['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Disability'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    
    # ---------------------------------------------------------------
    ubr_core_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [Total_Registered, ubr_overall_c, ubr_ethnicity_c, ubr_age_at_consent_c, ubr_sex_c, ubr_sexual_gender_minority_c,
         ubr_income_c, ubr_education_c, ubr_geography_c, ubr_disability_c])
    ubr_core_df = ubr_core_df.drop(['Total Registered'], axis=1)
    
    ubr_core_df = ubr_core_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core Participant')].astype(float), axis=0)
    ubr_core_df = ubr_core_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_df.columns:
        ubr_core_df = ubr_core_df.rename(columns={c: ('UBR of Core Participants', c)})
    
    ubr_core_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_df.columns))
    
    # ---------------------------------------------------------------
    #### UBR of Core -PM Participants
    # ---------------------------------------------------------------
    ubr_overall_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Overall'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_ethnicity_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Racial Identity'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name'])
    ubr_age_at_consent_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Age'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_sex_cpm = data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Sex'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_sexual_gender_minority_cpm = data_org.loc[
        (data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'SGM'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_income_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Income'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_education_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Education'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_geography_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Geography'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    ubr_disability_cpm = \
    data_org.loc[(data_org['enrollment_status'] == 'CORE_MINUS_PM') & (data_org['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name'])['participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Disability'}).sort_values(['organization_type', 'awardee_name', 'organization_name'])
    
    # ---------------------------------------------------------------
    ubr_core_minus_pm_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [Total_Registered, ubr_overall_cpm, ubr_ethnicity_cpm, ubr_age_at_consent_cpm, ubr_sex_cpm,
         ubr_sexual_gender_minority_cpm, ubr_income_cpm, ubr_education_cpm, ubr_geography_cpm, ubr_disability_cpm])
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.drop(['Total Registered'], axis=1)
    
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core -PM')].astype(float), axis=0)
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_minus_pm_df.columns:
        ubr_core_minus_pm_df = ubr_core_minus_pm_df.rename(columns={c: ('UBR of Core -PM Participants', c)})
    
    ubr_core_minus_pm_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_minus_pm_df.columns))
    
    # ---------------------------------------------------------------
    # Both UBR metrics
    ubr_dfs = reduce(lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
                     [ubr_core_df, ubr_core_minus_pm_df])
    
    # ---------------------------------------------------------------
    #### Gender Identity/Racial Identity/Age
    # ---------------------------------------------------------------
    demog_org = demog.copy()
    demog_org['organization_name'] = demog_org['organization_name'].fillna('zUnpaired')
    
    # ---------------------------------------------------------------
    
    gender_df = demog_org.pivot_table(values='participant_id',
                                      index=['organization_type', 'awardee_name', 'organization_name'],
                                      columns='gender_identity', aggfunc='nunique').reset_index()
    race_df = demog_org.pivot_table(values='participant_id',
                                    index=['organization_type', 'awardee_name', 'organization_name'],
                                    columns='race_ethnicity', aggfunc='nunique').reset_index()
    age_df = demog_org.pivot_table(values='participant_id',
                                   index=['organization_type', 'awardee_name', 'organization_name'], columns='age_group',
                                   aggfunc='nunique').reset_index()
    
    # ---------------------------------------------------------------
    # Gender
    gender_df = gender_df[
        ['organization_type', 'awardee_name', 'organization_name', 'Man', 'Non-Binary', 'Other/Addl. Options',
         'Transgender', 'Woman', 'Multiple Selections', 'Skipped', 'Prefer not to say']]
    gender_df = gender_df.set_index(['organization_type', 'awardee_name', 'organization_name'])
    
    for c in gender_df.columns:
        gender_df = gender_df.rename(columns={c: ('Gender Identity', c)})
    
    gender_df.columns = pd.MultiIndex.from_tuples(tuple(gender_df.columns))
    
    # Race
    race_df = race_df[
        ['organization_type', 'awardee_name', 'organization_name', 'Asian', 'Black and Hispanic, Latino, or Spanish',
         'Black or African American', 'Hispanic, Latino, or Spanish', 'Middle Eastern or North African',
         'More than one race', 'More than one race and Hispanic, Latino, or Spanish',
         'Native Hawaiian or other Pacific Islander', 'One other race and Hispanic, Latino, or Spanish', 'Other race',
         'Prefer not to say', 'White', 'White and Hispanic, Latino, or Spanish', 'Skipped']]
    race_df = race_df.set_index(['organization_type', 'awardee_name', 'organization_name'])
    
    for c in race_df.columns:
        race_df = race_df.rename(columns={c: ('Racial Identity', c)})
    
    race_df.columns = pd.MultiIndex.from_tuples(tuple(race_df.columns))
    
    # Age
    age_df = age_df.set_index(['organization_type', 'awardee_name', 'organization_name'])
    
    for c in age_df.columns:
        age_df = age_df.rename(columns={c: ('Age', c)})
    
    age_df.columns = pd.MultiIndex.from_tuples(tuple(age_df.columns))
    
    # ---------------------------------------------------------------
    # All Demographics
    demog_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [gender_df, race_df, age_df])
    demog_df = demog_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    #### Combined dfs
    # ---------------------------------------------------------------
    all_dfs_tab2_orgs = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name'], how='left'),
        [total_df, enroll_full_df, ubr_dfs, demog_df]).fillna(0)
    all_dfs_tab2_orgs.index = all_dfs_tab2_orgs.index.rename(['Type', 'Awardee', 'Organization'])
    all_dfs_tab2_orgs = all_dfs_tab2_orgs.rename(
        index={'HPO': 'RMC', 'UNSET': 'UNPAIRED', 'No organization set': 'Unpaired',
               'United States Department of Veteran Affairs': 'VA',
               'Cherokee Health Systems': 'Cherokee', 'Community Health Center, Inc': 'Community Health Center',
               'Eau Claire Cooperative Health Center': 'Eau Claire', 'Hudson River Health Care, Inc.': 'HRHCare',
               'Jackson-Hinds Comprehensive Health Center': 'Jackson-Hinds', 'San Ysidro Health Center': 'San Ysidro',
               'California Precision Medicine Consortium': 'California',
               'New England Precision Medicine Consortium': 'New England',
               'Pittsburgh': 'PITT', 'Southern Consortium': 'Southern',
               'Trans-American Consortium for the Health Care Systems Research Network (TACH)': 'Trans-America',
               'University of Texas Health Science Center at Houston': 'UT_HEALTH',
               'Virginia Commonwealth University': 'VCU',
               'Washington University in St. Louis': 'WASH U', 'Wisconsin Consortium': 'Wisconsin', 'Quest Labs': 'Quest'})
    
    # ---------------------------------------------------------------
    ### Sites
    # ---------------------------------------------------------------
    #### Enrollment Status (non-overlapping)
    # ---------------------------------------------------------------
    data_site = data.copy()
    data_site['site_name'] = data_site['site_name'].fillna('zUnpaired')
    
    # ---------------------------------------------------------------
    Total_Registered = data_site.groupby(['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Total Registered'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name']).astype(float)
    Total_Participants_Consented = data_site.loc[(data['total_participants_consented'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(
        columns={'participant_id': 'Total Participants (Consented)'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name']).astype(float)
    
    total_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [Total_Registered, Total_Participants_Consented])
    
    for c in total_df.columns:
        total_df = total_df.rename(columns={c: ('', c)})
    
    total_df.columns = pd.MultiIndex.from_tuples(tuple(total_df.columns))
    
    total_df = total_df.reindex(['Overall', 'HPO', 'FQHC', 'DV', 'VA', 'HPO-Lite', 'UNSET'], level='organization_type')
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    # enrollment from yesterday
    enrollment_df = data_site.pivot_table(values='participant_id',
                                          index=['organization_type', 'awardee_name', 'organization_name', 'site_name'],
                                          columns='enrollment_status', aggfunc='nunique').reset_index()
    enrollment_df = enrollment_df[
        ['organization_type', 'awardee_name', 'organization_name', 'site_name', 'REGISTERED', 'PARTICIPANT',
         'FULLY_CONSENTED', 'CORE_MINUS_PM', 'CORE_PARTICIPANT']]
    enrollment_df = enrollment_df.rename(
        columns={'REGISTERED': 'Registered Individuals', 'PARTICIPANT': 'Enrolled Participants',
                 'FULLY_CONSENTED': 'Participant +EHR', 'CORE_MINUS_PM': 'Total Core -PM',
                 'CORE_PARTICIPANT': 'Total Core Participant'}).set_index(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    # enrollment since previous report run
    enroll_sinceLast_rp_df = data_site.pivot_table(values='participant_id',
                                                   index=['organization_type', 'awardee_name', 'organization_name',
                                                          'site_name'], columns='enrollment_status_since_last_report',
                                                   aggfunc='nunique').reset_index()
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df[
        ['organization_type', 'awardee_name', 'organization_name', 'site_name', 'REGISTERED', 'PARTICIPANT',
         'FULLY_CONSENTED', 'CORE_MINUS_PM', 'CORE_PARTICIPANT']]
    enroll_sinceLast_rp_df = enroll_sinceLast_rp_df.rename(
        columns={'REGISTERED': 'Registered_prev', 'PARTICIPANT': 'Participant_prev',
                 'FULLY_CONSENTED': 'Participant +EHR_prev', 'CORE_MINUS_PM': 'Core -PM_prev',
                 'CORE_PARTICIPANT': 'Core Participant_prev'}).set_index(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    # weekly tally
    weekly_tally_df = data_site.pivot_table(values='participant_id',
                                            index=['organization_type', 'awardee_name', 'organization_name', 'site_name'],
                                            columns='weekly_tally', aggfunc='nunique')
    weekly_tally_df = weekly_tally_df.rename(columns={'CORE_MINUS_PM': 'CPM_prev_week', 'CORE_PARTICIPANT': 'CP_prev_week'})
    
    # merge
    enroll_full_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [enrollment_df, enroll_sinceLast_rp_df, weekly_tally_df])
    enroll_full_df.loc['Overall', :] = enroll_full_df.sum().values
    enroll_full_df = enroll_full_df.rename(index={'': 'All Awardees'})
    enroll_full_df = enroll_full_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # Date cols
    enroll_full_df['Registered Since Last Report'] = enroll_full_df['Registered Individuals'] - enroll_full_df[
        'Registered_prev']
    enroll_full_df['Participant Since Last Report'] = enroll_full_df['Enrolled Participants'] - enroll_full_df[
        'Participant_prev']
    enroll_full_df['Participant +EHR Since Last Report'] = enroll_full_df['Participant +EHR'] - enroll_full_df[
        'Participant +EHR_prev']
    enroll_full_df['Since Last Report'] = enroll_full_df['Total Core -PM'] - enroll_full_df['Core -PM_prev']
    enroll_full_df['CP Since Last Report'] = enroll_full_df['Total Core Participant'] - enroll_full_df[
        'Core Participant_prev']
    enroll_full_df['Weekly tally'] = enroll_full_df['Total Core -PM'] - enroll_full_df['CPM_prev_week']
    enroll_full_df['CP Weekly Tally'] = enroll_full_df['Total Core Participant'] - enroll_full_df['CP_prev_week']
    enroll_full_df = enroll_full_df.drop(
        ['Registered_prev', 'Participant_prev', 'Participant +EHR_prev', 'Core -PM_prev', 'Core Participant_prev',
         'CPM_prev_week', 'CP_prev_week'], axis=1)
    enroll_full_df = enroll_full_df[
        ['Registered Individuals', 'Registered Since Last Report', 'Enrolled Participants', 'Participant Since Last Report',
         'Participant +EHR', 'Participant +EHR Since Last Report', 'Total Core -PM', 'Since Last Report',
         'Weekly tally', 'Total Core Participant', 'CP Since Last Report', 'CP Weekly Tally']]
    
    for c in enroll_full_df.columns:
        enroll_full_df = enroll_full_df.rename(columns={c: ('Enrollment Status (non-overlapping)', c)})
    
    enroll_full_df.columns = pd.MultiIndex.from_tuples(tuple(enroll_full_df.columns))
    
    # ---------------------------------------------------------------
    ####  UBR of Core Participants
    # ---------------------------------------------------------------
    ubr_overall_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Overall'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_ethnicity_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Racial Identity'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_age_at_consent_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Age'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_sex_c = data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Sex'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_sexual_gender_minority_c = data_site.loc[
        (data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'SGM'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_income_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Income'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_education_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Education'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_geography_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Geography'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_disability_c = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_PARTICIPANT') & (data_site['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Disability'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    # ---------------------------------------------------------------
    ubr_core_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [Total_Registered, ubr_overall_c, ubr_ethnicity_c, ubr_age_at_consent_c, ubr_sex_c, ubr_sexual_gender_minority_c,
         ubr_income_c, ubr_education_c, ubr_geography_c, ubr_disability_c])
    ubr_core_df = ubr_core_df.drop(['Total Registered'], axis=1)
    
    ubr_core_df = ubr_core_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core Participant')].astype(float), axis=0)
    ubr_core_df = ubr_core_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_df.columns:
        ubr_core_df = ubr_core_df.rename(columns={c: ('UBR of Core Participants', c)})
    
    ubr_core_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_df.columns))
    
    # ---------------------------------------------------------------
    #### UBR of Core -PM Participants
    # ---------------------------------------------------------------
    ubr_overall_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_overall'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Overall'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_ethnicity_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_ethnicity'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Racial Identity'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_age_at_consent_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_age_at_consent'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Age'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_sex_cpm = data.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_sex'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Sex'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_sexual_gender_minority_cpm = data_site.loc[
        (data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_sexual_gender_minority'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'SGM'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_income_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_income'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Income'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_education_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_education'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Education'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_geography_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_geography'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Geography'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    ubr_disability_cpm = \
    data_site.loc[(data_site['enrollment_status'] == 'CORE_MINUS_PM') & (data_site['ubr_disability'] == 1)].groupby(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])[
        'participant_id'].nunique().to_frame().rename(columns={'participant_id': 'Disability'}).sort_values(
        ['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    # ---------------------------------------------------------------
    ubr_core_minus_pm_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [Total_Registered, ubr_overall_cpm, ubr_ethnicity_cpm, ubr_age_at_consent_cpm, ubr_sex_cpm,
         ubr_sexual_gender_minority_cpm, ubr_income_cpm, ubr_education_cpm, ubr_geography_cpm, ubr_disability_cpm])
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.drop(['Total Registered'], axis=1)
    
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.div(
        enroll_full_df[('Enrollment Status (non-overlapping)', 'Total Core -PM')].astype(float), axis=0)
    ubr_core_minus_pm_df = ubr_core_minus_pm_df.fillna(0)
    pd.set_option('display.float_format', '{:.2%}'.format)
    
    for c in ubr_core_minus_pm_df.columns:
        ubr_core_minus_pm_df = ubr_core_minus_pm_df.rename(columns={c: ('UBR of Core -PM Participants', c)})
    
    ubr_core_minus_pm_df.columns = pd.MultiIndex.from_tuples(tuple(ubr_core_minus_pm_df.columns))
    
    # ---------------------------------------------------------------
    # Both UBR metrics
    ubr_dfs = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [ubr_core_df, ubr_core_minus_pm_df])
    
    # ---------------------------------------------------------------
    ####  Gender Identity/Racial Identity/Age
    # ---------------------------------------------------------------
    demog_site = demog.copy()
    demog_site['site_name'] = demog_org['site_name'].fillna('zUnpaired')
    
    # ---------------------------------------------------------------
    gender_df = demog_site.pivot_table(values='participant_id',
                                       index=['organization_type', 'awardee_name', 'organization_name', 'site_name'],
                                       columns='gender_identity', aggfunc='nunique').reset_index()
    race_df = demog_site.pivot_table(values='participant_id',
                                     index=['organization_type', 'awardee_name', 'organization_name', 'site_name'],
                                     columns='race_ethnicity', aggfunc='nunique').reset_index()
    age_df = demog_site.pivot_table(values='participant_id',
                                    index=['organization_type', 'awardee_name', 'organization_name', 'site_name'],
                                    columns='age_group', aggfunc='nunique').reset_index()
    
    # ---------------------------------------------------------------
    # Gender Identity
    gender_df = gender_df[
        ['organization_type', 'awardee_name', 'organization_name', 'site_name', 'Man', 'Non-Binary', 'Other/Addl. Options',
         'Transgender', 'Woman', 'Multiple Selections', 'Skipped', 'Prefer not to say']]
    gender_df = gender_df.set_index(['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    for c in gender_df.columns:
        gender_df = gender_df.rename(columns={c: ('Gender Identity', c)})
    
    gender_df.columns = pd.MultiIndex.from_tuples(tuple(gender_df.columns))
    
    # Race Identity
    race_df = race_df[['organization_type', 'awardee_name', 'organization_name', 'site_name', 'Asian',
                       'Black and Hispanic, Latino, or Spanish', 'Black or African American',
                       'Hispanic, Latino, or Spanish', 'Middle Eastern or North African', 'More than one race',
                       'More than one race and Hispanic, Latino, or Spanish', 'Native Hawaiian or other Pacific Islander',
                       'One other race and Hispanic, Latino, or Spanish', 'Other race', 'Prefer not to say', 'White',
                       'White and Hispanic, Latino, or Spanish', 'Skipped']]
    race_df = race_df.set_index(['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    for c in race_df.columns:
        race_df = race_df.rename(columns={c: ('Racial Identity', c)})
    
    race_df.columns = pd.MultiIndex.from_tuples(tuple(race_df.columns))
    
    # Age
    age_df = age_df.set_index(['organization_type', 'awardee_name', 'organization_name', 'site_name'])
    
    for c in age_df.columns:
        age_df = age_df.rename(columns={c: ('Age', c)})
    
    age_df.columns = pd.MultiIndex.from_tuples(tuple(age_df.columns))
    
    # ---------------------------------------------------------------
    # All Demographics
    demog_df = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [gender_df, race_df, age_df])
    demog_df = demog_df.fillna(0)
    pd.set_option('display.float_format', '{:,.0f}'.format)
    
    # ---------------------------------------------------------------
    ### Combined dfs
    # ---------------------------------------------------------------
    all_dfs_tab2_sites = reduce(
        lambda x, y: pd.merge(x, y, on=['organization_type', 'awardee_name', 'organization_name', 'site_name'], how='left'),
        [total_df, enroll_full_df, ubr_dfs, demog_df]).fillna(0)
    all_dfs_tab2_sites.index = all_dfs_tab2_sites.index.rename(['Type', 'Awardee', 'Organization', 'Site'])
    all_dfs_tab2_sites = all_dfs_tab2_sites.rename(
        index={'HPO': 'RMC', 'UNSET': 'UNPAIRED', 'No organization set': 'Unpaired',
               'United States Department of Veteran Affairs': 'VA',
               'Cherokee Health Systems': 'Cherokee', 'Community Health Center, Inc': 'Community Health Center',
               'Eau Claire Cooperative Health Center': 'Eau Claire', 'Hudson River Health Care, Inc.': 'HRHCare',
               'Jackson-Hinds Comprehensive Health Center': 'Jackson-Hinds', 'San Ysidro Health Center': 'San Ysidro',
               'California Precision Medicine Consortium': 'California',
               'New England Precision Medicine Consortium': 'New England',
               'Pittsburgh': 'PITT', 'Southern Consortium': 'Southern',
               'Trans-American Consortium for the Health Care Systems Research Network (TACH)': 'Trans-America',
               'University of Texas Health Science Center at Houston': 'UT_HEALTH',
               'Virginia Commonwealth University': 'VCU',
               'Washington University in St. Louis': 'WASH U', 'Wisconsin Consortium': 'Wisconsin', 'Quest Labs': 'Quest'})
    
    # ---------------------------------------------------------------
    # Merge & formatting for tab 2
    tab2_orgs = all_dfs_tab2_orgs.reset_index(level=2, col_level=1)
    tab2_sites = all_dfs_tab2_sites.reset_index(level=[2, 3], col_level=1)
    
    custom_sort = (all_dfs_tab1.columns)
    tab2_all = pd.concat([all_dfs_tab1, tab2_orgs, tab2_sites], sort=False)
    tab2_all = tab2_all.sort_index().reset_index().sort_values(['Awardee', ('', 'Organization'), ('', 'Site')],
                                                               na_position='first')
    tab2_all = tab2_all.set_index(['Type', 'Awardee', ('', 'Organization'), ('', 'Site')])
    tab2_all = tab2_all[all_dfs_tab1.columns]
    tab2_all = tab2_all.reindex(['Overall', 'RMC', 'FQHC', 'DV', 'VA', 'HPO-Lite', 'UNPAIRED'], level='Type')
    tab2_all.index.names = ['Type', 'Awardee', 'Organization', 'Site']
    # tab2_all[('Age', '0-17')] = tab2_all[('Age', '0-17')].fillna(0)
    
    # Move ubr_disability to the end
    ubr_disabilities = tab2_all.iloc[:, tab2_all.columns.get_level_values(1) == 'Disability']
    tab2_all = tab2_all.drop([('UBR of Core Participants', 'Disability'), ('UBR of Core -PM Participants', 'Disability')],
                             axis=1)
    tab2_all = reduce(lambda x, y: pd.merge(x, y, on=['Type', 'Awardee', 'Organization', 'Site'], how='left'),
                      [tab2_all, ubr_disabilities])
    
    tab2_all = tab2_all.rename(
        columns={'Registered Since Last Report': 'Since Last Report', 'Participant Since Last Report': 'Since Last Report',
                 'Participant +EHR Since Last Report': 'Since Last Report'})
    tab2_all = tab2_all.rename(index={'zUnpaired': 'Unpaired'})
    tab2_all.drop(('UNPAIRED', 'Unpaired', 'Unpaired'), axis=0, inplace=True)
    
    # Extra columns from Scott's report
    tab2_all.insert(loc=9, column=('Enrollment Status (non-overlapping)', 'Since Reactivation'), value='-')
    tab2_all.insert(loc=13, column=('Enrollment Status (non-overlapping)', 'CP Since Reactivation'), value='-')
    tab2_all.insert(loc=16, column=(' ', '# Days Since Reactivation'), value='-')
    
    # no data 0-17 age bucket
    tab2_all.insert(loc=55, column=('Age', '0-17'), value=0)
    return tab1_final, tab2_all

    # ---------------------------------------------------------------
    # file_name = yday + '_' + 'PEO-Participant_Enrollment_Overview' + '.xlsx'
    # writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    # tab1_final.to_excel(writer, sheet_name='PEO Report', startrow=2)
    # tab2_all.to_excel(writer, sheet_name='Including Orgs & Sites', startrow=2)
    # wb = writer.book
    # ws1 = writer.sheets['PEO Report']
    # ws2 = writer.sheets['Including Orgs & Sites']
    #
    # # Cell formats
    # num_format = wb.add_format({'num_format': '#,##0'})
    # pct_format = wb.add_format({'num_format': '0.00%'})
    # idx_format = wb.add_format({'align': 'left', 'valign': 'top'})
    # a1_format = wb.add_format({'font_size': 12, 'bold': True})
    #
    # ws1.write('A1', 'Participant Enrollment Overview', a1_format)
    # ws1.write('A2', 'Updated: ')
    # ws1.write('B2', yday)
    # ws1.set_column('A:A', 10, idx_format)
    # ws1.set_column('B:B', 26, idx_format)
    # ws1.set_column('C:S', 8.5, num_format)
    # ws1.set_column('T:AI', 8.5, pct_format)
    # ws1.set_column('AJ:BN', 8.5, num_format)
    # ws1.set_column('BO:BP', 8.5, pct_format)
    # ws1.set_column('L:L', None, None, {'hidden': True})
    # ws1.set_column('P:P', None, None, {'hidden': True})
    # ws1.set_column('S:S', None, None, {'hidden': True})
    #
    # ws2.write('A1', 'Participant Enrollment Overview', a1_format)
    # ws2.write('A2', 'Updated: ')
    # ws2.write('B2', yday)
    # ws2.set_column('A:A', 10, idx_format)
    # ws2.set_column('B:B', 26, idx_format)
    # ws2.set_column('C:C', 56, idx_format)
    # ws2.set_column('D:D', 90, idx_format)
    # ws2.set_column('E:U', 8.5, num_format)
    # ws2.set_column('V:AK', 8.5, pct_format)
    # ws2.set_column('AL:BP', 8.5, num_format)
    # ws2.set_column('BQ:BR', 8.5, pct_format)
    # ws2.set_column('N:N', None, None, {'hidden': True})
    # ws2.set_column('R:R', None, None, {'hidden': True})
    # ws2.set_column('U:U', None, None, {'hidden': True})
    #
    # writer.save()


def run(sa_conn):
    """
    Build the PEO reports.
    :param sa_conn: sqlalchemy database engine object.
    :return: PEO Report, Orgs Report
    """
    return generate_peo_report(sa_conn)
