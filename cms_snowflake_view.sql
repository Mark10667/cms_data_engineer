-- DROP VIEW IF EXISTS cms_database_1.cms_schema.cms_dac_aggregated;

CREATE OR REPLACE VIEW cms_database_1.cms_schema.dac_npi_agg AS
SELECT 
    base.NPI,
    base.gndr AS Gender,
    base.Telehlth AS Telehealth,
    cred_subquery.First_Cred,
    med_sch_subquery.Med_School_with_Max_Exp,
    med_sch_subquery.Max_Years_exp
FROM 
    (SELECT DISTINCT NPI, gndr, Telehlth FROM cms_database_1.cms_schema.cms_dac) base
LEFT JOIN 
    (SELECT DISTINCT NPI, 
            FIRST_VALUE(Cred) OVER (PARTITION BY NPI ORDER BY Ind_enrl_ID) AS First_Cred 
     FROM cms_database_1.cms_schema.cms_dac) cred_subquery
ON base.NPI = cred_subquery.NPI
LEFT JOIN 
    (SELECT DISTINCT NPI, 
            FIRST_VALUE(Med_sch) OVER (PARTITION BY NPI ORDER BY Years_exp DESC, Ind_enrl_ID) AS Med_School_with_Max_Exp,
            MAX(Years_exp) OVER (PARTITION BY NPI) AS Max_Years_exp
     FROM cms_database_1.cms_schema.cms_dac) med_sch_subquery
ON base.NPI = med_sch_subquery.NPI;


SELECT *
FROM cms_database_1.cms_schema.dac_npi_agg LIMIT 10;

SELECT COUNT(*) FROM cms_database_1.cms_schema.dac_npi_agg



-- DROP VIEW IF EXISTS cms_database_1.cms_schema.combined_org;

CREATE OR REPLACE VIEW cms_database_1.cms_schema.gorup_pt_exp_map AS
SELECT 
    f.org_pac_id,
    f.measure_cd,
    f.measure_title,
    f.prf_rate,
    f.patient_count,
    d.State,
    d.City,
    d.num_org_mem
FROM 
    cms_database_1.cms_schema.CMS_PT_EXP f
LEFT JOIN 
    (SELECT DISTINCT org_pac_id as org_pac_id, State, City, num_org_mem
     FROM cms_database_1.cms_schema.cms_dac) d
ON 
    TRY_CAST(f.org_pac_id AS FLOAT) = TRY_CAST(d.org_pac_id AS FLOAT)



SELECT *
FROM cms_database_1.cms_schema.gorup_pt_exp_map order by org_pac_id, STATE, CITY, MEASURE_CD LIMIT 20;

SELECT COUNT(DISTINCT org_pac_id) FROM cms_database_1.cms_schema.cms_pt_exp



CREATE OR REPLACE VIEW cms_database_1.cms_schema.cms_dac_state_view AS
SELECT DISTINCT 
    NPI, 
    Last_name, 
    First_name, 
    gndr, 
    Cred, 
    Med_sch, 
    Grd_yr, 
    pri_spec, 
    Telehlth, 
    org_pac_id, 
    State, 
    Years_exp
FROM 
    cms_database_1.cms_schema.cms_dac;


SELECT *
FROM cms_database_1.cms_schema.cms_dac_state_view order by NPI LIMIT 20;

SELECT COUNT(*) FROM cms_database_1.cms_schema.cms_dac_state_view





CREATE OR REPLACE VIEW cms_database_1.cms_schema.cms_mips_state_view AS
SELECT 
    dac.NPI,
    dac.org_pac_id,
    dac.State,
    dac.City,
    mips.source, 
    mips.Quality_category_score,
    mips.PI_category_score, 
    mips.IA_category_score,
    mips.final_MIPS_score_without_CPB,
    mips.final_MIPS_score
FROM cms_database_1.cms_schema.cms_dac dac
INNER JOIN cms_database_1.cms_schema.cms_mips mips
ON dac.NPI = mips.NPI AND dac.org_pac_id = mips.org_pac_id;

