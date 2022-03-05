use role accountadmin;
use warehouse covid_19_compute;
use database covid_19;
use schema myschema;

list @covid_19.myschema.covid_19_data;

create or replace table covid_deaths
(index int,
data_as_of date,
start_date date,
end_date date,
group_method char(20),
state char(20),
sex char(20),
age_group varchar(30),
covid_19_deaths int,
total_deaths int,
pneumonia_deaths int,
pneumonia_and_covid_19_deaths int,
influenza_deaths int,
pneumonia_influenza_or_covid int,
footnotes varchar(256),
report_year date,
test varchar(256)
);

create or replace table covid_deaths_Test
(index varchar(256),
data_as_of varchar(256),
start_date varchar(256),
end_date varchar(256),
group_method varchar(256),
state varchar(256),
sex varchar(256),
age_group varchar(256),
covid_19_deaths varchar(256),
total_deaths varchar(256),
pneumonia_deaths varchar(256),
pneumonia_and_covid_19_deaths varchar(256),
influenza_deaths varchar(256),
pneumonia_influenza_or_covid varchar(256),
footnotes varchar(256),
report_year varchar(256)
--test varchar(256)
);

copy into covid_deaths_Test
from @covid_19_data
file_format = (
type = csv
skip_header = 1
field_delimiter = ','
record_delimiter = '\n'
empty_field_as_null=true
error_on_column_count_mismatch=false);

select * from covid_deaths_test
where index = '2246';