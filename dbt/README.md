IMT reservation history analysis, tables, based on dbt starter project

### Author

Olli Salli <ollisal@gmail.com>

### History

#### 2025/10/05

- dbt connected to Redshift
- Added relevant ERP table exports from S3 using Redshift Spectrum in erp_raw datasource

#### 2025/10/06

- Defined staging models for reservation, trip, triphase

#### 2025/10/07

- Defined int_tripproductstep - decoding of product selection steps by index
- Builtin data tests for int_tripproductstep

### Using the starter project

Try running the following commands:

- dbt run
- dbt test

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
