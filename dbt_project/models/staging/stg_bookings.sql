-- ============================================================
-- stg_bookings.sql
-- Goal: Staging model for silver_bookings
-- Layer: Staging (view)
-- Author: Esra Demirturk Duman
-- ============================================================

-- Neden staging modeli?
-- Silver'dan Gold'a direkt geçmiyoruz.
-- Staging = Silver'ı Gold için hazırlıyoruz:
-- - Kolon isimlerini standardize ediyoruz
-- - İş kurallarına göre basit dönüşümler yapıyoruz
-- - Her mart modeli staging'den okur, Silver'dan değil

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_bookings') }}
),

staged AS (
    SELECT
        -- Keys
        booking_id,
        hotel_id,
        customer_id,

        -- Dates
        booking_date,
        checkin_date,
        checkout_date,

        -- Stay details
        nights,
        adults,
        COALESCE(children, 0)               AS children,
        COALESCE(infants, 0)                AS infants,
        adults + COALESCE(children, 0)      AS total_guests,
        room_type,
        rooms_booked,

        -- Booking info
        booking_channel,
        booking_source,
        booking_status,
        special_requests,

        -- Cancellation
        is_cancelled,
        cancellation_date,
        cancellation_reason,

        -- Financials
        COALESCE(total_price, 0)            AS total_price,
        COALESCE(room_price, 0)             AS room_price,
        COALESCE(tax_amount, 0)             AS tax_amount,
        COALESCE(service_fee, 0)            AS service_fee,
        COALESCE(paid_amount, 0)            AS paid_amount,
        payment_status,
        payment_method,

        -- Promotions
        promotion_code,
        COALESCE(discount_amount, 0)        AS discount_amount,

        -- Metadata
        silver_updated_at

    FROM source
    WHERE booking_id IS NOT NULL
      AND hotel_id IS NOT NULL
      AND customer_id IS NOT NULL
)

SELECT * FROM staged
