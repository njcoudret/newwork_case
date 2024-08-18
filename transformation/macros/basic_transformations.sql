{%- macro bool(column) -%}
IF(cast({{column}} as varchar) IN ('1','Yes'),TRUE,FALSE)
{%- endmacro -%}

{%- macro set_null(column) -%}
NULLIF({{column}},'000000000000000AAA')
{%- endmacro -%}

{%- macro c_date(column) -%}
CAST({{column}} as date)
{%- endmacro -%}

{%- macro c_int(column) -%}
CAST({{column}} as integer)
{%- endmacro -%}

{%- macro to_local_tz(column) -%}
((cast({{column}} as timestamp) at time zone 'utc') at time zone '{{var("dbt_date:time_zone")}}')
{%- endmacro -%}



