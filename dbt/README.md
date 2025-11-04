# imt-reservations-bi

Tables for IMT.fi reservation history analysis, built using dbt (could also be used for interactive BI)

## Author

Olli Salli <https://www.linkedin.com/in/ollisal/>

## History

### 2024/11/04

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
