IMT reservation history analysis, tables, based on dbt starter project

### Author

Olli Salli https://www.linkedin.com/in/ollisal/

### History

#### 2025/10/05

- dbt connected to Redshift
- Added relevant ERP table exports from S3 using Redshift Spectrum in erp_raw datasource

#### 2025/10/06

- Defined staging models for reservation, trip, triphase

#### 2025/10/07

- Defined int_tripproductstep - decoding of product selection steps by index
- Builtin data tests for int_tripproductstep
