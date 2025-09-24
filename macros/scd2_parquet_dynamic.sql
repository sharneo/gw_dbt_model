{% macro scd2_parquet_dynamic(parquet_path, id_col='ID', seq_col=null, change_col='CHANGE_INDICATOR', incremental=false) %}

{#-----------------------------------#}
{# Step 1: Read raw Parquet data      #}
{#-----------------------------------#}
with raw as (
    select *
    from parquet.`{{ parquet_path }}`
    {% if incremental %}
        where {{ change_col }} in ('INSERT','UPDATE')
    {% endif %}
),

{#-----------------------------------#}
{# Step 2: Detect all column names dynamically #}
{#-----------------------------------#}
cols as (
    select column_name
    from lateral flatten(input=>object_keys(raw[0])) -- pseudo-code, see note below
),

{#-----------------------------------#}
{# Step 3: Align columns for expired and new_current #}
{#-----------------------------------#}
expired as (
    select
    {% for col in cols %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    , false as CURRENT_FLAG
    , (select min(UpdateTime) from raw r2 where r2.{{ id_col }} = raw.{{ id_col }}) as EFFECTIVE_TO
    from {{ this }}
    where CURRENT_FLAG = TRUE
      and {{ id_col }} in (select {{ id_col }} from raw)
),

new_current as (
    select
    {% for col in cols %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    , TRUE as CURRENT_FLAG
    , UpdateTime as EFFECTIVE_FROM
    , to_timestamp_ltz('9999-12-31') as EFFECTIVE_TO
    from raw
)

select * from expired
union all
select * from new_current

{% endmacro %}
