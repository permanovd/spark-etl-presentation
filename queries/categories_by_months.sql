select ref_date, category, statement_component, sum(CAST(value AS DOUBLE)) as value
from report_data_raw
group by ref_date, category, statement_component;