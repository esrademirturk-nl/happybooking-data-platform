-- ============================================================
-- fact_booking.sql
-- Goal: Core fact table for booking analytics
-- Layer: Gold (table)
-- Author: Esra Demirturk Duman
-- ============================================================

-- Neden fact table?
-- Data Warehouse'da "star schema" kullanıyoruz.
-- Fact table = ölçülebilir iş olayları (rezervasyonlar)
-- Dimension table = fact'i tanımlayan bilgiler (otel, müşteri)
-- Power BI bu yapıyla çok daha hızlı çalışır.

WITH bookings AS (
    SELECT * FROM {{ ref('stg_bookings') }}
),

hotels AS (
    SELECT * FROM {{ ref('stg_hotels') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        -- Keys
        b.booking_id,
        b.hotel_id,
        b.customer_id,

        -- Dates
        b.booking_date,
        b.checkin_date,
        b.checkout_date,
        YEAR(b.checkin_date)                AS checkin_year,
        MONTH(b.checkin_date)               AS checkin_month,
        DATENAME(MONTH, b.checkin_date)     AS checkin_month_name,
        DATENAME(WEEKDAY, b.checkin_date)   AS checkin_day_name,

        -- Stay details
        b.nights,
        b.adults,
        b.children,
        b.infants,
        b.total_guests,
        b.room_type,
        b.rooms_booked,

        -- Booking info
        b.booking_channel,
        b.booking_source,
        b.booking_status,
        b.is_cancelled,
        b.cancellation_reason,

        -- Financials
        b.total_price,
        b.room_price,
        b.tax_amount,
        b.service_fee,
        b.paid_amount,
        b.discount_amount,
        b.payment_method,
        b.payment_status,

        -- Revenue calculations
        -- Neden: Power BI'da hesaplamak yerine burada yapıyoruz
        -- daha hızlı ve tutarlı
        b.total_price - b.discount_amount   AS net_revenue,
        CASE
            WHEN b.nights > 0 
            THEN b.total_price / b.nights
            ELSE 0
        END                                 AS revenue_per_night,
        CASE
            WHEN b.total_guests > 0
            THEN b.total_price / b.total_guests
            ELSE 0
        END                                 AS revenue_per_guest,

        -- Hotel context
        h.hotel_name,
        h.hotel_type,
        h.star_rating,
        h.country                           AS hotel_country,
        h.city                              AS hotel_city,

        -- Customer context
        c.loyalty_level,
        c.gender                            AS customer_gender,
        c.country                           AS customer_country,
        c.language_preference,

        -- Metadata
        b.silver_updated_at,
        GETDATE()                           AS gold_updated_at

    FROM bookings b
    LEFT JOIN hotels h ON b.hotel_id = h.hotel_id
    LEFT JOIN customers c ON b.customer_id = c.customer_id
)

SELECT * FROM final
