-- ============================================================
-- stg_customers.sql
-- Goal: Staging model for silver_customers
-- Layer: Staging (view)
-- Author: Esra Demirturk Duman
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_customers') }}
),

staged AS (
    SELECT
        -- Keys
        customer_id,

        -- Personal info
        first_name,
        last_name,
        full_name,
        email,
        phone,
        birth_date,
        gender,

        -- Location
        country_customer                    AS country,
        city_customer                       AS city,
        address,
        postal_code,

        -- Preferences
        language_preference,
        COALESCE(loyalty_level_customer, 
                 'Bronze')                  AS loyalty_level,
        marketing_consent,

        -- Dates
        registration_date,

        -- Metadata
        silver_updated_at

    FROM source
    WHERE customer_id IS NOT NULL
      AND email IS NOT NULL
)

SELECT * FROM staged
