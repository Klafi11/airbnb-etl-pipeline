from dataclasses import dataclass, field
from typing import Optional
from datetime import date, datetime

@dataclass
class AirbnbRecord:
    id: int
    name: str
    host_id: int
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    latitude: float
    longitude: float
    room_type: str
    price: float
    minimum_nights: int
    number_of_reviews: int
    last_review: date
    reviews_per_month: float
    calculated_host_listings_count: int
    availability_365: int
    number_of_reviews_ltm: int
    license: str

    processed_at: datetime = field(init=False)

    def validate(self): 
        
        errors = []

        # --- Required field checks ---
        if not self.id: 
            errors.append("Missing id")

        if not self.name or self.name.strip() == "":
            errors.append("Missing name")

        if self.neighbourhood_group is None or self.neighbourhood_group.strip() == "":
            errors.append("Missing Neigbourhood_group")

        if self.neighbourhood is None or self.neighbourhood.strip() == "":
            errors.append("Missing Neigbourhood")

        if self.latitude is None: 
            errors.append("Missing latitude")

        if self.longitude is None: 
            errors.append("Missing Longitude")

        if not self.room_type or self.room_type.strip() == "":
            errors.append("Missing room_type")

        if self.price is None:
            errors.append("Missing price")

        if self.minimum_nights is None or self.minimum_nights < 1:
            errors.append(f"Invalid minimum_nights ({self.minimum_nights}) | must be ≥ 1")

        if self.number_of_reviews is None or self.number_of_reviews < 0:
            errors.append(f"Invalid number_of_reviews ({self.number_of_reviews}) | cannot be negative")

        if self.reviews_per_month is None or self.reviews_per_month < 0:
            errors.append(f"Invalid reviews_per_month ({self.reviews_per_month}) | cannot be negative")

        if self.calculated_host_listings_count is None or self.calculated_host_listings_count < 0:
            errors.append(f"Invalid calculated_host_listings_count ({self.calculated_host_listings_count}) | cannot be negative")

        if self.availability_365 is None or not (0 <= self.availability_365 <= 365):
            errors.append(f"Invalid availability_365 ({self.availability_365}) | must be between 0 and 365")

        if self.license is None or len(self.license.strip()) == 0:
            errors.append("License field is blank | None ")

        return errors

    