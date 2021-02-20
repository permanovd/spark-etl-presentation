CREATE TABLE IF NOT exists finance.categories_by_months
(
ref_date DATE, category VARCHAR, statement_component VARCHAR, value DECIMAL,
PRIMARY KEY ((ref_date, category), statement_component)
)