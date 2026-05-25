-- ============================================================
-- kpi_revenue.sql
-- Goal: Revenue KPIs for Power BI dashboard
-- Layer: Gold (table)
-- Author: Esra Demirturk Duman
-- ============================================================

-- Neden ayrı KPI tablosu?
-- Power BI'da her seferinde karmaşık hesaplamalar yapmak yerine
-- önceden hesaplanmış KPI'lar çok daha hızlı çalışır.
-- Bu tablo günlük/aylık revenue analizleri için kullanılır.

WITH bookings AS (
    SELECT * FROM {{ ref('fact_booking') }}
    WHERE is_cancelled = FALSE
),

monthly_revenue AS (
    SELECT
        checkin_year                        AS year,
        checkin_month                       AS month,
        checkin_month_name                  AS month_name,
        hotel_country,
        hotel_city,
        hotel_category,
        room_type,
        booking_channel,

        -- Volume metrics
        COUNT(booking_id)                   AS total_bookings,
        COUNT(DISTINCT hotel_id)            AS active_hotels,
        COUNT(DISTINCT customer_id)         AS unique_customers,
        SUM(nights)                         AS total_nights,
        SUM(total_guests)                   AS total_guests,

        -- Revenue metrics
        SUM(total_price)                    AS gross_revenue,
        SUM(net_revenue)                    AS net_revenue,
        SUM(discount_amount)                AS total_discounts,
        SUM(tax_amount)                     AS total_tax,
        SUM(service_fee)                    AS total_service_fee,

        -- Average metrics
        AVG(total_price)                    AS avg_booking_value,
        AVG(revenue_per_night)              AS avg_revenue_per_night,
        AVG(revenue_per_guest)              AS avg_revenue_per_guest,
        AVG(nights)                         AS avg_stay_length,

        -- Metadata
        GETDATE()                           AS gold_updated_at

    FROM bookings
    GROUP BY
        checkin_year,
        checkin_month,
        checkin_month_name,
        hotel_country,
        hotel_city,
        hotel_category,
        room_type,
        booking_channel
),

with_growth AS (
    SELECT
        *,
        -- Month over month growth
        -- Neden: Trend analizi için kritik KPI
        LAG(gross_revenue) OVER (
            PARTITION BY hotel_city, room_type
            ORDER BY year, month
        )                                   AS prev_month_revenue,

        CASE
            WHEN LAG(gross_revenue) OVER (
                PARTITION BY hotel_city, room_type
                ORDER BY year, month
            ) > 0
            THEN ROUND(
                (gross_revenue - LAG(gross_revenue) OVER (
                    PARTITION BY hotel_city, room_type
                    ORDER BY year, month
                )) * 100.0 / LAG(gross_revenue) OVER (
                    PARTITION BY hotel_city, room_type
                    ORDER BY year, month
                ), 2)
            ELSE 0
        END                                 AS mom_growth_pct

    FROM monthly_revenue
)

SELECT * FROM with_growth
