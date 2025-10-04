# E-commerce Event-Driven Data Pipeline

## Project Overview

This project implements a comprehensive event-driven data pipeline for e-commerce transactional data processing using Databricks, PySpark, and Delta Lake. The pipeline handles multiple data sources with advanced data engineering patterns including SCD2 (Slowly Changing Dimensions), data validation, enrichment, and automated archiving.

## Tech Stack

- **Databricks** - Cloud-based data processing platform
- **PySpark** - Distributed data processing engine
- **Delta Lake** - ACID transactions and versioning for data lakes
- **Databricks Volumes** - Managed storage for file-based data sources
- **Databricks Workflows** - Job orchestration and scheduling
- **GitHub** - Version control and CI/CD

## Project Structure

```
Ecomm_Event_Driven_Datapipeline/
├── data/
│   ├── customers_2025_01_15.csv
│   ├── customers_2025_01_16.csv
│   ├── inventory_2025_01_15.csv
│   ├── inventory_2025_01_16.csv
│   ├── orders_2025_01_15.csv
│   ├── orders_2025_01_16.csv
│   ├── products_2025_01_15.csv
│   ├── products_2025_01_16.csv
│   ├── shipping_2025_01_15.csv
│   ├── shipping_2025_01_16.csv
│   ├── trigger_2025_01_15.json
│   └── trigger_2025_01_16.json
└── notebook/
    ├── 01_orders_stage_load.ipynb
    ├── 02_customers_stage_load.ipynb
    ├── 03_products_stage_load.ipynb
    ├── 04_inventory_stage_load.ipynb
    ├── 05_shipping_stage_load.ipynb
    ├── 06_data_validation.ipynb
    ├── 07_data_enrichment.ipynb
    └── 08_final_merge_operation.ipynb
```

## Pipeline Architecture

### 1. Data Ingestion Layer
- **Source Files**: CSV files for orders, customers, products, inventory, and shipping data
- **Trigger Files**: JSON configuration files that initiate batch processing
- **Storage**: Databricks Volumes for managed file storage

### 2. Data Processing Layer
- **Stage Load Notebooks**: Process and validate raw data files
- **Validation Notebook**: Cross-reference validation and business rule checks
- **Enrichment Notebook**: Add business metrics and analytics
- **Merge Notebook**: SCD2 implementation for target tables

### 3. Data Storage Layer
- **Staging Tables**: Temporary storage for validated data
- **Target Tables**: Final storage with historical versioning
- **Analytics Tables**: Business intelligence and reporting tables

## Notebook Details

### 01_orders_stage_load.ipynb
- Loads order data from CSV files
- Validates order amounts, customer IDs, and product IDs
- Archives processed files
- Logs processing summary

### 02_customers_stage_load.ipynb
- Processes customer demographic data
- Validates email formats and phone numbers
- Calculates customer age and lifecycle stages
- Enriches data with customer segments

### 03_products_stage_load.ipynb
- Loads product catalog information
- Validates product prices and categories
- Calculates product lifecycle metrics
- Enriches with product performance indicators

### 04_inventory_stage_load.ipynb
- Processes inventory stock levels
- Validates stock quantities and status
- Tracks inventory movements
- Updates stock status indicators

### 05_shipping_stage_load.ipynb
- Loads shipping and logistics data
- Validates shipping costs and addresses
- Tracks package weights and dimensions
- Enriches with shipping metrics

### 06_data_validation.ipynb
- Cross-reference validation across all tables
- Business rule validation (premium customers, discontinued products)
- Orphaned record detection
- Comprehensive validation scoring system

### 07_data_enrichment.ipynb
- Creates enriched orders dataset with all related information
- Calculates customer analytics (total orders, spending, segments)
- Generates product analytics (revenue, performance categories)
- Adds business metrics (profit margins, CLV estimates)

### 08_final_merge_operation.ipynb
- Implements SCD2 merge logic for all target tables
- Creates analytics summary dashboard
- Generates seasonal and segment analysis
- Finalizes data processing pipeline

## Data Models

### Source Data Schema

#### Orders
- order_id, customer_id, product_id
- order_date, order_amount, currency
- payment_method, shipping_address, order_status
- created_timestamp

#### Customers
- customer_id, first_name, last_name, email, phone
- date_of_birth, registration_date, address
- city, state, zip_code, country
- customer_tier, last_login, created_timestamp

#### Products
- product_id, product_name, category, subcategory, brand
- price, currency, stock_quantity, weight_kg
- dimensions_cm, color, material, description
- launch_date, discontinued, created_timestamp

#### Inventory
- product_id, stock_quantity, stock_status
- warehouse_location, last_updated, created_timestamp

#### Shipping
- order_id, shipping_method, shipping_cost, currency
- tracking_number, package_weight, dimensions_cm
- estimated_delivery, actual_delivery, created_timestamp

### Target Tables with SCD2
- **orders_target**: Historical order data with effective/expiry dates
- **customers_target**: Customer dimension with versioning
- **products_target**: Product catalog with historical changes
- **inventory_target**: Inventory snapshots over time
- **shipping_target**: Shipping history tracking

### Analytics Tables
- **analytics_summary**: Overall business KPIs
- **customer_analytics**: Customer segmentation and metrics
- **product_analytics**: Product performance analysis
- **seasonal_analysis**: Seasonal trend analysis
- **segment_analysis**: Customer segment performance

## Setup Instructions

### Prerequisites
1. Databricks workspace with appropriate permissions
2. Delta Lake enabled
3. Databricks Volumes configured
4. GitHub integration set up

### Configuration Steps

1. **Create Databricks Catalog and Schema**
```sql
CREATE CATALOG IF NOT EXISTS `demo-external-catalog`;
CREATE SCHEMA IF NOT EXISTS `demo-external-catalog`.default;
```

2. **Set up Volume Mounts**
```python
# Configure volume paths for data sources
source_dirs = {
    "orders": "/Volumes/demo-external-catalog/default/incremental_load/orders_data/source/",
    "customers": "/Volumes/demo-external-catalog/default/incremental_load/customers_data/source/",
    "products": "/Volumes/demo-external-catalog/default/incremental_load/products_data/source/",
    "inventory": "/Volumes/demo-external-catalog/default/incremental_load/inventory_data/source/",
    "shipping": "/Volumes/demo-external-catalog/default/incremental_load/shipping_data/source/"
}
```

3. **Upload Sample Data**
- Upload CSV files to respective source directories
- Upload JSON trigger files to initiate processing

4. **Create Databricks Workflow**
- Set up multi-task job with notebook dependencies
- Configure file arrival triggers
- Set up monitoring and alerting

## Usage

### Manual Execution
1. Upload data files to source directories
2. Create trigger JSON file with batch metadata
3. Execute notebooks in sequence:
   - Stage load notebooks (01-05)
   - Data validation (06)
   - Data enrichment (07)
   - Final merge (08)

### Automated Execution
1. Set up Databricks Workflow with file arrival triggers
2. Configure automatic job execution on file upload
3. Monitor processing logs and alerts

## Monitoring and Logging

### Processing Logs
- All processing steps log to `processing_log` table
- Includes timestamps, record counts, and status
- Error tracking and validation results

### Validation Results
- Comprehensive validation scoring in `validation_results` table
- Severity levels: HIGH, MEDIUM, LOW, NONE
- Detailed error descriptions and counts

### Business Metrics
- Real-time analytics in summary tables
- Customer and product performance tracking
- Revenue and profit margin calculations

## Data Quality Features

### Validation Rules
- **Data Completeness**: Required field validation
- **Data Format**: Email, phone, date format validation
- **Business Rules**: Premium customer behavior, discontinued products
- **Referential Integrity**: Cross-table relationship validation

### Error Handling
- Invalid records logged to error tables
- Processing continues with valid data
- Detailed error reporting and investigation

## Performance Optimization

### Delta Lake Features
- ACID transactions for data consistency
- Schema evolution support
- Time travel and versioning
- Optimized storage and compression

### PySpark Optimizations
- Partitioned data processing
- Broadcast joins for dimension tables
- Efficient data types and schemas
- Parallel processing across clusters

## Business Intelligence

### Key Metrics
- Total orders and revenue
- Average order value
- Customer lifetime value
- Product performance scores
- Seasonal trend analysis

### Customer Segmentation
- **VIP**: High spending, frequent orders
- **High Value**: Moderate spending, regular orders
- **Medium Value**: Average spending, occasional orders
- **Low Value**: Low spending, infrequent orders

### Product Categories
- **Star**: High revenue, frequent orders
- **High Performer**: Good revenue and order frequency
- **Medium Performer**: Average performance
- **Low Performer**: Below average metrics

## Troubleshooting

### Common Issues
1. **File Upload Errors**: Check volume permissions and file formats
2. **Schema Validation**: Verify CSV headers match expected schema
3. **Data Quality**: Review validation results and error logs
4. **Performance**: Monitor cluster resources and query execution

### Debug Steps
1. Check processing logs for error details
2. Review validation results for data quality issues
3. Examine error tables for invalid records
4. Verify trigger file format and metadata

## Future Enhancements

### Planned Features
- Real-time streaming data ingestion
- Machine learning model integration
- Advanced customer behavior analytics
- Automated anomaly detection
- Real-time dashboard updates

### Scalability Improvements
- Auto-scaling cluster configuration
- Dynamic partition management
- Advanced caching strategies
- Multi-region deployment support

## Contributing

### Development Guidelines
1. Follow PySpark best practices
2. Maintain comprehensive logging
3. Include data validation checks
4. Update documentation for changes

### Testing
- Unit tests for data transformations
- Integration tests for pipeline components
- Data quality validation tests
- Performance benchmarking

## License

This project is part of the Data Engineering 5.0 curriculum and is intended for educational purposes.

## Contact

For questions or support regarding this project, please refer to the course documentation or contact the instructor.
