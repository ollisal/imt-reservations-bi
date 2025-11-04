# AI Agent Instructions for IMT Reservations BI

This project is a dbt-based data pipeline for analyzing the reservation funnel for a travel company's website, focusing on trip bookings and conversion analysis.

## Project Structure

- `dbt/models/`: Core data transformations organized in layers:
  - `staging/`: Raw data cleanup (e.g., `stg_reservation.sql`, `stg_trip.sql`)
  - `intermediate/`: Business logic transformations (e.g., `int_tripproductstep.sql`)
  - `mart/`: Analytics-ready tables (e.g., `fct_reservation_funnel.sql`)
- `notebooks/`: Jupyter notebooks for analysis (e.g., `funnel_analysis.ipynb`)

## Key Concepts

### Reservation Funnel Stages
The system tracks user progression through booking stages:
1. ProductSelection (with sub-steps for departure/hotel/ship selections)
2. PassengerInfo
3. ReserverInfo
4. AdditionalServices
5. Confirmation
6. Confirmed

See `fct_reservation_funnel.sql` for the funnel progress scoring logic.

### Product Steps Pattern
Trip bookings follow a structured pattern encoded in `int_tripproductstep.sql`:
- Each trip has multiple phases (hotels/ships)
- Product selection happens in order: departure → flight? -> hotel+room -> ship+cabin
- Steps are indexed sequentially for funnel analysis

## Development Workflow

### Setting Up
1. Configure Redshift credentials in `~/.dbt/profiles.yml`
2. Install dependencies with pip install -r requirements.txt

### Common Commands
```bash
dbt run     # Build all models
dbt test    # Run data quality tests
dbt build   # Run and test together
```

### Data Analysis
- Use Jupyter notebooks for analysis (see `funnel_analysis.ipynb` for connection pattern)
- Key tables:
  - `fct_reservation_funnel`: Main analysis table with funnel metrics
  - `int_tripproductstep`: Decoded product selection steps

## Best Practices

1. Model Organization:
   - Use staging models for raw data cleanup
   - Build intermediate models for complex business logic
   - Create mart models for analytics use cases

2. Performance:
   - Use appropriate Redshift distribution keys (see `fct_reservation_funnel.sql`)
   - Consider table vs incremental materialization based on data volume

3. Testing:
   - Add data tests in `schema.yml` files
   - Test relationships between models (see `models/intermediate/schema.yml`)
   - Use new format for test definition with "arguments" key

## Common Patterns

- Indent width is 4 spaces for SQL and Python, 2 for YAML
- Use CTE-based SQL structure for readability
- Follow dbt naming conventions: stg_, int_, fct_ prefixes
- Implement proper Redshift optimization settings in model configs
- Use single quotes for normal strings (double quotes only for SQL identifiers and such).
- Do not use unnecessary quotes at all in YAML files.