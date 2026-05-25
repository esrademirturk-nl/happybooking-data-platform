-- ============================================================
-- stg_hotels.sql
-- Goal: Staging model for silver_hotels
-- Layer: Staging (view)
-- Author: Esra Demirturk Duman
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'silver_hotels') }}
),

staged AS (
    SELECT
        -- Keys
        hotel_id,

        -- Hotel info
        COALESCE(hotel_name, 'Unknown')     AS hotel_name,
        COALESCE(hotel_type, 'Unknown')     AS hotel_type,
        COALESCE(star_rating, 0)            AS star_rating,
        COALESCE(total_rooms, 0)            AS total_rooms,

        -- Location
        country,
        city,
        COALESCE(latitude, 0)               AS latitude,
        COALESCE(longitude, 0)              AS longitude,

        -- Details
        hotel_facilities,
        hotel_description,
        nearby_attractions,
        website,

        -- Metadata
        silver_updated_at

    FROM source
    WHERE hotel_id IS NOT NULL
)

SELECT * FROM staged
