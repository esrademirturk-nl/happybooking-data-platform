# ============================================================
# test_quality.py
# Goal: Pytest tests for data quality validation
# Author: Esra Demirturk Duman
# ============================================================

# Neden Pytest?
# CI/CD pipeline'da GE testleri Fabric dışında da çalışabilsin diye
# Pandas ile basit unit testler yazıyoruz.
# Bu testler GitHub Actions'da koşar, Fabric'e gerek yok.

import pytest
import pandas as pd
from datetime import datetime

# --------------------------------------------------
# 1. Sample Test Data (mock Silver data)
# --------------------------------------------------
# Neden mock data: CI/CD'de gerçek Fabric'e bağlanamayız
# Bunun yerine Silver verinin yapısını test ediyoruz

@pytest.fixture
def sample_bookings():
    return pd.DataFrame({
        "booking_id":    ["B001", "B002", "B003"],
        "hotel_id":      ["H001", "H002", "H003"],
        "customer_id":   ["C001", "C002", "C003"],
        "checkin_date":  ["2024-01-01", "2024-02-01", "2024-03-01"],
        "checkout_date": ["2024-01-05", "2024-02-03", "2024-03-07"],
        "nights":        [4, 2, 6],
        "total_price":   [500.0, 200.0, 750.0],
        "is_cancelled":  [False, False, True],
        "booking_status":["Confirmed", "Confirmed", "Cancelled"]
    })

@pytest.fixture
def sample_hotels():
    return pd.DataFrame({
        "hotel_id":    ["H001", "H002", "H003"],
        "hotel_name":  ["Grand Hotel", "City Inn", "Beach Resort"],
        "star_rating": [5, 3, 4],
        "total_rooms": [200, 80, 150],
        "country":     ["NL", "FR", "ES"],
        "city":        ["Amsterdam", "Paris", "Barcelona"]
    })

@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id":   ["C001", "C002", "C003"],
        "email":         ["a@test.com", "b@test.com", "c@test.com"],
        "gender":        ["Male", "Female", "Other"],
        "loyalty_level": ["Gold", "Bronze", "Silver"]
    })

@pytest.fixture
def sample_reviews():
    return pd.DataFrame({
        "review_id":     ["R001", "R002", "R003"],
        "hotel_id":      ["H001", "H002", "H003"],
        "review_rating": [8.5, 7.0, 9.0],
        "is_verified_review": [True, True, False]
    })

# --------------------------------------------------
# 2. Bookings Tests
# --------------------------------------------------

class TestBookings:

    def test_booking_id_not_null(self, sample_bookings):
        # Neden: booking_id olmayan kayıt analiz edilemez
        assert sample_bookings["booking_id"].isnull().sum() == 0, \
            "booking_id contains NULL values"

    def test_booking_id_unique(self, sample_bookings):
        # Neden: Duplicate booking_id veri bütünlüğünü bozar
        assert sample_bookings["booking_id"].nunique() == len(sample_bookings), \
            "booking_id is not unique"

    def test_hotel_id_not_null(self, sample_bookings):
        assert sample_bookings["hotel_id"].isnull().sum() == 0, \
            "hotel_id contains NULL values"

    def test_customer_id_not_null(self, sample_bookings):
        assert sample_bookings["customer_id"].isnull().sum() == 0, \
            "customer_id contains NULL values"

    def test_total_price_positive(self, sample_bookings):
        # Neden: Negatif fiyat iş mantığına aykırı
        assert (sample_bookings["total_price"] >= 0).all(), \
            "total_price contains negative values"

    def test_nights_positive(self, sample_bookings):
        # Neden: 0 veya negatif geceleme geçersiz
        assert (sample_bookings["nights"] > 0).all(), \
            "nights contains non-positive values"

    def test_is_cancelled_boolean(self, sample_bookings):
        assert sample_bookings["is_cancelled"].dtype == bool, \
            "is_cancelled should be boolean"

# --------------------------------------------------
# 3. Hotels Tests
# --------------------------------------------------

class TestHotels:

    def test_hotel_id_not_null(self, sample_hotels):
        assert sample_hotels["hotel_id"].isnull().sum() == 0, \
            "hotel_id contains NULL values"

    def test_hotel_id_unique(self, sample_hotels):
        assert sample_hotels["hotel_id"].nunique() == len(sample_hotels), \
            "hotel_id is not unique"

    def test_hotel_name_not_null(self, sample_hotels):
        assert sample_hotels["hotel_name"].isnull().sum() == 0, \
            "hotel_name contains NULL values"

    def test_star_rating_range(self, sample_hotels):
        # Neden: 1-5 dışındaki puan geçersiz
        assert sample_hotels["star_rating"].between(1, 5).all(), \
            "star_rating contains values outside 1-5 range"

    def test_total_rooms_positive(self, sample_hotels):
        assert (sample_hotels["total_rooms"] > 0).all(), \
            "total_rooms contains non-positive values"

# --------------------------------------------------
# 4. Customers Tests
# --------------------------------------------------

class TestCustomers:

    def test_customer_id_not_null(self, sample_customers):
        assert sample_customers["customer_id"].isnull().sum() == 0, \
            "customer_id contains NULL values"

    def test_customer_id_unique(self, sample_customers):
        assert sample_customers["customer_id"].nunique() == len(sample_customers), \
            "customer_id is not unique"

    def test_email_not_null(self, sample_customers):
        assert sample_customers["email"].isnull().sum() == 0, \
            "email contains NULL values"

    def test_email_unique(self, sample_customers):
        assert sample_customers["email"].nunique() == len(sample_customers), \
            "email is not unique"

    def test_gender_valid_values(self, sample_customers):
        valid_genders = ["Male", "Female", "Other"]
        assert sample_customers["gender"].isin(valid_genders).all(), \
            "gender contains invalid values"

    def test_loyalty_level_valid_values(self, sample_customers):
        valid_levels = ["Platinum", "Gold", "Silver", "Bronze"]
        assert sample_customers["loyalty_level"].isin(valid_levels).all(), \
            "loyalty_level contains invalid values"

# --------------------------------------------------
# 5. Reviews Tests
# --------------------------------------------------

class TestReviews:

    def test_review_id_not_null(self, sample_reviews):
        assert sample_reviews["review_id"].isnull().sum() == 0, \
            "review_id contains NULL values"

    def test_review_id_unique(self, sample_reviews):
        assert sample_reviews["review_id"].nunique() == len(sample_reviews), \
            "review_id is not unique"

    def test_review_rating_range(self, sample_reviews):
        # Neden: 1-10 dışındaki puan geçersiz
        assert sample_reviews["review_rating"].between(1, 10).all(), \
            "review_rating contains values outside 1-10 range"

    def test_is_verified_boolean(self, sample_reviews):
        assert sample_reviews["is_verified_review"].dtype == bool, \
            "is_verified_review should be boolean"
