

{% snapshot customer_snapshot %}

  {{
    config(
      target_schema='STG_TPCH_CUSTOMERS',
      unique_key='customer_key',
      strategy='check',
      invalidate_hard_deletes=True,
      check_cols=['name', 'address', 'phone_number']
    )
  }}



with source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select
    
        c_custkey as customer_key,
        c_name as name,
        c_address as address, 
        c_nationkey as nation_key,
        c_phone as phone_number,
        c_acctbal as account_balance,
        c_mktsegment as market_segment,
        c_comment as comment

    from source

)

select * from renamed

{% endsnapshot %}
