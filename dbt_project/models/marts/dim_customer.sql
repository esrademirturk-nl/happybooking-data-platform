-- ============================================================
-- dim_customer.sql
-- Goal: Customer dimension table for star schema
-- Layer: Gold (table)
-- Author: Esra Demirturk Duman
-- ============================================================

-- Neden dim_customer?
-- Müşteri segmentasyonu için temel tablo.
-- Loyalty level, yaş grubu, ülke bazında analizler
-- Power BI'da bu tablodan yapılır.

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

booking_summary AS (
    -- Neden: Her müşterinin toplam rezervasyon ve harcama bilgisi
    -- Customer lifetime value (CLV) hesabı için
    SELECT
        customer_id,
        COUNT(booking_id)                   AS total_bookings,
        SUM(total_price)                    AS total_spent,
        AVG(total_price)                    AS avg_booking_value,
        MIN(checkin_date)                   AS first_stay,
        MAX(checkin_date)                   AS last_stay,
        SUM(CASE WHEN is_cancelled = 1 
            THEN 1 ELSE 0 END)              AS total_cancellations
    FROM {{ ref('stg_bookings') }}
    GROUP BY customer_id
),

final AS (
    SELECT
        -- Keys
        c.customer_id,

        -- Personal info
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.gender,
        c.birth_date,

        -- Age calculation
        -- Neden: Yaş bazında segmentasyon için
        DATEDIFF(YEAR, c.birth_date, GETDATE()) AS age,
        CASE
            WHEN DATEDIFF(YEAR, c.birth_date, GETDATE()) < 30 
                THEN 'Young (18-29)'
            WHEN DATEDIFF(YEAR, c.birth_date, GETDATE()) < 45 
                THEN 'Middle (30-44)'
            WHEN DATEDIFF(YEAR, c.birth_date, GETDATE()) < 60 
                THEN 'Senior (45-59)'
            ELSE 'Elder (60+)'
        END                                 AS age_group,

        -- Location
        c.country,
        c.city,
        c.language_preference,

        -- Loyalty
        c.loyalty_level,
        CASE
            WHEN c.loyalty_level = 'Platinum' THEN 4
            WHEN c.loyalty_level = 'Gold'     THEN 3
            WHEN c.loyalty_level = 'Silver'   THEN 2
            ELSE 1
        END                                 AS loyalty_rank,

        -- Marketing
        c.marketing_consent,
        c.registration_date,

        -- Booking metrics
        COALESCE(b.total_bookings, 0)       AS total_bookings,
        COALESCE(b.total_spent, 0)          AS total_spent,
        COALESCE(b.avg_booking_value, 0)    AS avg_booking_value,
        b.first_stay,
        b.last_stay,
        COALESCE(b.total_cancellations, 0)  AS total_cancellations,

        -- Customer value segment
        CASE
            WHEN COALESCE(b.total_spent, 0) >= 10000 THEN 'High Value'
            WHEN COALESCE(b.total_spent, 0) >= 5000  THEN 'Mid Value'
            ELSE 'Low Value'
        END                                 AS customer_segment,

        -- Metadata
        c.silver_updated_at,
        GETDATE()                           AS gold_updated_at

    FROM customers c
    LEFT JOIN booking_summary b ON c.customer_id = b.customer_id
)

SELECT * FROM final
