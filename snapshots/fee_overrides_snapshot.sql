{% snapshot fee_overrides_snapshot %}

{{
    config(
        unique_key='change_id',
        strategy='check',
        check_cols=['fee_multiplier', 'scheduled_at'],
    )
}}

    select * from {{ ref('stg_fee_overrides') }}

{% endsnapshot %}
