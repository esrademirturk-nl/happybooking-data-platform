-- ============================================================
-- dim_hotel.sql
-- Goal: Hotel dimension table for star schema
-- Layer: Gold (table)
-- Author: Esra Demirturk Duman
-- ============================================================

-- Neden dimension table?
-- Fact table'daki hotel_id'yi zenginleştirmek için.
-- Power BI'da "hotel bazında analiz" yapabilmek için
-- tüm otel bilgileri burada toplanır.

WITH hotels AS (
    SELECT * FROM {{ ref('stg_hotels') }}
),

reviews_summary AS (
    -- Neden: Her otelin ortalama puanını dim'e ekliyoruz
    -- Power BI'da ayrı join yapmak zorunda kalmayız
    SELECT
        hotel_id,
        COUNT(review_id)                    AS total_reviews,
        AVG(CAST(review_rating AS FLOAT))   AS avg_rating,
        MIN(review_rating)                  AS min_rating,
        MAX(review_rating)                  AS max_rating
    FROM {{ ref('stg_reviews') }}
    GROUP BY hotel_id
),

final AS (
    SELECT
        -- Keys
        h.hotel_id,

        -- Hotel info
        h.hotel_name,
        h.hotel_type,
        h.star_rating,
        h.total_rooms,

        -- Location
        h.country,
        h.city,
        h.latitude,
        h.longitude,

        -- Details
        h.hotel_facilities,
        h.nearby_attractions,
        h.website,

        -- Review metrics
        COALESCE(r.total_reviews, 0)        AS total_reviews,
        COALESCE(r.avg_rating, 0)           AS avg_review_rating,
        COALESCE(r.min_rating, 0)           AS min_review_rating,
        COALESCE(r.max_rating, 0)           AS max_review_rating,

        -- Categorization
        -- Neden: Power BI'da filtreleme kolaylaşır
        CASE
            WHEN h.star_rating >= 4 THEN 'Luxury'
            WHEN h.star_rating = 3 THEN 'Mid-Range'
            ELSE 'Budget'
        END                                 AS hotel_category,

        CASE
            WHEN h.total_rooms >= 200 THEN 'Large'
            WHEN h.total_rooms >= 100 THEN 'Medium'
            ELSE 'Small'
        END                                 AS hotel_size,

        -- Metadata
        h.silver_updated_at,
        GETDATE()                           AS gold_updated_at

    FROM hotels h
    LEFT JOIN reviews_summary r ON h.hotel_id = r.hotel_id
)

SELECT * FROM final
