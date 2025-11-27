# imt-reservations-bi

Tables for IMT.fi reservation history analysis, built using dbt (could also be used for interactive BI)

## Author

Olli Salli <https://www.linkedin.com/in/ollisal/>

## History

### 2025/11/27

- Add departureyear to int_reservationpassengerbio
- No longer try to distinguish "definite" adults/children, we can filter by having exact ages and/or reservation abandon stage elsewhere anyway

### 2025/11/24

- Incremental refresh for stg_passenger, stg_person
- Create int_reservationpassengerbio, which infers passenger ages in multiple ways
- Ignore mega-ancient Person date-of-births, which are really anonymization sentinel values

### 2025/11/22

- Add stg_passenger, including data tests

### 2025/11/21

- Data tests for stg_reservation and stg_person

### 2025/11/19

- Add stg_person, with some anonymization

### 2025/11/14

- Add reached_* shorthand properties to fct_reservation_funnel
- Fix two Flight TripPhases being counted as generating two product selection steps; in reality outgoing and return flight are chosen together

### 2025/11/13

- Add int_triptypepriority, which normalizes trip type classification differences
- Add trip type classification to dim_trip
- Add similar int_tripdestinationpriority, and use it to bring destination info to dim_trip

### 2025/11/11

- Add stg_triptype trivial staging model
- Add flattened int_travelstephierarchy model

### 2025/11/10

- Add stg_travelstep and stg_tripdestination staging models

### 2025/11/07

- Ensure business appropriate date boundary handling for reservation create/confirmation/modifytime
- Add less error prone date-only versions of above columns

### 2025/11/05

- Patch some ProductSelection reservations which have undeniably progressed past last product selection step, to indicate abandonment in PassengerInfo
- Indicate abandon step being unknown for very old reservations, and reservations whose trip is known to have been since totally revamped

### 2025/11/04

- Add dim_trip with number of trip phases info, to be expanded

### 2025/11/03

- Add flight trip handling to int_tripproductstep

### 2025/10/31

- Move to dbt Fusion engine

### 2025/10/12

- created fct_reservation_funnel

### 2025/10/08

- added int_tripproductstep

### 2025/10/05

- dbt connected to Redshift
- Added relevant ERP table exports from S3 using Redshift Spectrum in erp_raw datasource

### 2025/10/06

- Defined staging models for reservation, trip, triphase

### 2025/10/07

- Defined int_tripproductstep - decoding of product selection steps by index
- Builtin data tests for int_tripproductstep
