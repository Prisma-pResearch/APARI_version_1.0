# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 11:26:47 2024

@author: ruppert20
"""
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.Database.connect_to_database import execute_query_in_transaction
import pandas as pd

def upload_rvu_data(engine_bundle, rvu_workbook_fp: str):
    
    execute_query_in_transaction(query=f'''IF OBJECT_ID(N'{engine_bundle.lookup_schema}.[PHYSICIAN_RVU_Lookup]', N'U') IS NULL
                                        CREATE TABLE {engine_bundle.lookup_schema}.[PHYSICIAN_RVU_Lookup](
                                        	[omop_concept_id] [int] NULL,
                                        	[hcpcs_code] [char](5) NOT NULL,
                                        	[modifier] [varchar](55) NULL,
                                        	[description] [varchar](500) NULL,
                                        	[status_code] [char](1) NULL,
                                        	[not_used_for_medicare_payment] [bit] NULL,
                                        	[work_rvu] [float] NULL,
                                        	[transitioned_non_fac_pe_rvu] [float] NULL,
                                        	[transitioned_non_fac_na_indicator] [char](2) NULL,
                                        	[fully_implemented_non_fac_pe_rvu] [float] NULL,
                                        	[fully_implemented_non_fac_na_indicator] [char](2) NULL,
                                        	[transitioned_facility_na_indicator] [char](2) NULL,
                                        	[transitioned_facility_pe_rvu] [float] NULL,
                                        	[mp_rvu] [float] NULL,
                                        	[transitioned_non_facility_total] [float] NULL,
                                        	[fully_implemented_facility_na_indicator] [char](2) NULL,
                                        	[fully_implemented_non_facility_total] [float] NULL,
                                        	[transitioned_facility_total] [float] NULL,
                                        	[fully_implemented_facility_pe_rvu] [float] NULL,
                                        	[fully_implemented_facility_total] [float] NULL,
                                        	[pctc_ind] [int] NULL,
                                        	[glob_days] [char](3) NULL,
                                        	[pre_op] [float] NULL,
                                        	[intra_op] [float] NULL,
                                        	[post_op] [float] NULL,
                                        	[mult_proc] [int] NULL,
                                        	[bilat_surg] [int] NULL,
                                        	[asst_surg] [int] NULL,
                                        	[co_surg] [int] NULL,
                                        	[team_surg] [int] NULL,
                                        	[endo_base] [char](5) NULL,
                                        	[conv_factor] [float] NULL,
                                        	[physician_supervision_of_diagnostic_procedures] [char](2) NULL,
                                        	[calculation_flag] [bit] NULL,
                                        	[diagnostic_imaging_family_indicator] [int] NULL,
                                        	[non_facility_pe_used_for_opps_payment_amount] [float] NULL,
                                        	[facility_pe_used_for_opps_payment_amount] [float] NULL,
                                        	[mp_used_for_opps_payment_amount] [float] NULL,
                                        	[start_date] [date] NOT NULL,
                                        	[end_date] [date] NOT NULL,
                                        	[release] [char](5) NOT NULL,
                                        	[non_fac_pe_rvu] [float] NULL,
                                        	[non_fac_na_indicator] [char](2) NULL,
                                        	[facility_pe_rvu] [float] NULL,
                                        	[facility_na_indicator] [char](2) NULL,
                                        	[non_facility_total] [float] NULL,
                                        	[facility_total] [float] NULL,
                                         CONSTRAINT [PK_PHYSICIAN_RVU_Lookup] UNIQUE NONCLUSTERED 
                                        (
                                        	[hcpcs_code] ASC,
                                        	[release] ASC,
                                        	[omop_concept_id] ASC
                                        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                        ) ON [PRIMARY]''', engine=engine_bundle.engine)
                                        
    execute_query_in_transaction(query=f'''IF (SELECT COUNT(*) as index_count
                                            FROM sys.indexes 
                                            WHERE object_id = OBJECT_ID(N'{engine_bundle.lookup_schema}.[PHYSICIAN_RVU_Lookup]', N'U')
                                            AND name='IX_concept_lookup_PHYSICIAN_RVU_Lookup') IS NULL
                                        CREATE NONCLUSTERED INDEX [IX_concept_lookup_PHYSICIAN_RVU_Lookup] ON {engine_bundle.lookup_schema}.[PHYSICIAN_RVU_Lookup]
                                        (
                                        	[omop_concept_id] ASC,
                                        	[start_date] ASC,
                                        	[end_date] ASC
                                        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];''',
                                        engine=engine_bundle.engine)
                                            
    table_def: pd.DataFrame = pd.read_sql(f'''SELECT
                                    inf.COLUMN_NAME,
                                    inf.DATA_TYPE,
                                    inf.IS_NULLABLE,
                                    COALESCE(inf.CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) [CHARACTER_MAXIMUM_LENGTH]
                                FROM
                                    {engine_bundle.database}.sys.tables
                                    INNER JOIN {engine_bundle.database}.sys.columns ON tables.object_id = columns.object_id
                                    INNER JOIN {engine_bundle.database}.sys.types ON types.user_type_id = columns.user_type_id
                                    INNER JOIN {engine_bundle.database}.sys.schemas ON schemas.schema_id = tables.schema_id
                                    INNER JOIN {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS inf ON (inf.TABLE_SCHEMA=schemas.name
                                                                                            AND inf.TABLE_NAME=tables.name
                                                                                            AND inf.COLUMN_NAME=columns.name)
                                WHERE
                                   inf.table_name = 'PHYSICIAN_RVU_Lookup'
                                   AND
                                   inf.TABLE_SCHEMA = '{engine_bundle.lookup_schema.split('.')[-1]}'
                                   AND
                                   columns.is_identity <> 1;''', engine_bundle.engine)
                                   
    releases = pd.ExcelFile(rvu_workbook_fp).sheet_names
    
    hcpcs_map: pd.DataFrame = pd.concat([check_load_df(f'''SELECT
                                                    concept_code [hcpcs_code],
                                                    concept_id [omop_concept_id]
                                                FROM
                                                    {engine_bundle.vocab_schema}.CONCEPT
                                                WHERE
                                                    vocabulary_id = 'HCPCS'; ''',
                                                    engine=engine_bundle),
                                         check_load_df(f'''SELECT
                                                               concept_code [hcpcs_code],
                                                               concept_id [omop_concept_id]
                                                           FROM
                                                               {engine_bundle.vocab_schema}.CONCEPT
                                                           WHERE
                                                           vocabulary_id = 'CPT4'; ''',
                                                       engine=engine_bundle)], axis=0)
    for release in releases:
        if pd.read_sql(f"SELECT TOP 1 * FROM {engine_bundle.lookup_schema}.[PHYSICIAN_RVU_Lookup] WHERE release = '{release}';",
                       engine_bundle.engine).shape[0] == 0:
            print(f'Uploading: {release}')
            df = check_load_df(rvu_workbook_fp, sheet_name=release).rename(columns={'hcpcs': 'hcpcs_code',
                                                                                    'mod': 'modifier',
                                                                                    'status_code_': 'status_code', 
                                                                                    'start_date_': 'start_date'})\
                .merge(hcpcs_map, on='hcpcs_code', how='left')\
                .dropna(subset=['omop_concept_id'])
                
            assert len([x for x in df.columns if x not in table_def.COLUMN_NAME.values]) == 0, f'The following columns are unexpected: {[x for x in df.columns if x not in table_def.COLUMN_NAME.values]}'
                
            save_data(df[df.modifier.isnull()], dest_table='PHYSICIAN_RVU_Lookup', dest_schema=engine_bundle.lookup_schema, engine=engine_bundle)
