# Data Caveats

- **Price coverage is capped**: the scrape only captured properties up to about `$900k`, so anything above that is missing.
- **Land size is sparse**: `land_size_sqm` is missing for about 80% of rows.
- **Some variables aren't fully reported** `car_spaces`, `bedrooms`, and `bathrooms` were all under-reported for around 3-7% of rows.
- **SA2 join isn't perfect**: spatial mapping coverage is about `99.8%`; a small % remains unmapped.
- **Privacy**: public outputs exclude address, geometry, and parcel identifiers.
