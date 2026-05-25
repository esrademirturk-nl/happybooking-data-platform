-- ============================================================
-- stg_reviews.sql
-- Goal: Staging model for silver_reviews
-- Layer: Staging (view)
-- Author: Esra Demirturk Duman
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_reviews') }}
),

staged AS (
    SELECT
        -- Keys
        review_id,
        booking_id,
        hotel_id,
        customer_id,

        -- Review details
        COALESCE(review_rating, 0)          AS review_rating,
        review_title,
        review_text,
        review_date,
        COALESCE(helpful_votes, 0)          AS helpful_votes,
        is_verified_review,
        reviewer_location,

        -- Stay info
        stay_date,
        trip_type,
        room_type_reviewed,

        -- Metadata
        silver_updated_at

    FROM source
    WHERE review_id IS NOT NULL
      AND review_rating IS NOT NULL
)

SELECT * FROM staged
