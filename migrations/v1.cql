CREATE TABLE IF NOT exists finance.report_data_raw
(
ref_date DATE, geo VARCHAR, dguid VARCHAR,
category VARCHAR, statement_component VARCHAR, release VARCHAR, uom VARCHAR, scalar_factor VARCHAR,
vector VARCHAR, coordinate varchar, value varchar, status varchar, symbol varchar, terminated varchar,
decimals varchar,
PRIMARY KEY ((ref_date, category), statement_component, release)
)