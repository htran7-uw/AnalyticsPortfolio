use role accountadmin;
use warehouse covid_19_compute;
use database covid_19;
use schema myschema;

list @covid_19.myschema.covid_19_data;

--sorted
create or replace table covid_deaths
    (file_row_number int,
    age_group varchar(30),
    covid_19_deaths int,
    data_as_of date,
    end_date date,
    footnote varchar(256),
     group_method char(20), --name is group
     influenza_deaths int,
     month_number int, --name is month
     pneumonia_and_covid_19_deaths int,
     pneumonia_deaths int,
     pneumonia_influenza_or_covid int,
     sex char(20),
     start_date date,
     state char(20),
     total_deaths int,
     year_number int);

copy into covid_deaths
from @covid_19_data
file_format = (
type = csv
skip_header = 1
field_delimiter = ','
record_delimiter = '\n'
empty_field_as_null=true);
--error_on_column_count_mismatch=false);

select * from covid_deaths
where file_row_number = 6;