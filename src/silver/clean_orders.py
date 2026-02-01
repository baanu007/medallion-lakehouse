"""
Silver Layer: Clean Orders
Transforms raw orders from Bronze to cleaned Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrdersCleaner:
    """
    Cleans and transforms orders from Bronze to Silver layer
    
    Transformations applied:
    - Deduplication by order_id
    - Null handling
    - Data type standardization
    - Business rule validation
    """
    
    def __init__(
        self,
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        
    def read_bronze(self, start_version: Optional[int] = None):
        """
        Read data from Bronze layer
        
        Args:
            start_version: Optional version for incremental processing
        """
        logger.info(f"Reading from Bronze: {self.bronze_path}")
        
        if start_version is not None:
            # Incremental read using Change Data Feed
            return (
                self.spark.read
                .format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", start_version)
                .load(self.bronze_path)
                .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
            )
        else:
            return self.spark.read.format("delta").load(self.bronze_path)
    
    def deduplicate(self, df):
        """
        Remove duplicates keeping latest record per order_id
        """
        logger.info("Deduplicating orders...")
        
        window = Window.partitionBy("order_id").orderBy(F.desc("_ingestion_timestamp"))
        
        return (
            df
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
    
    def clean_data(self, df):
        """
        Apply data cleaning transformations
        """
        logger.info("Cleaning order data...")
        
        return (
            df
            # Standardize strings
            .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
            .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
            
            # Parse and validate dates
            .withColumn("order_date", F.to_date(F.col("order_date")))
            .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
            
            # Ensure numeric types
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
            .withColumn("discount_amount", 
                       F.coalesce(F.col("discount_amount").cast("decimal(10,2)"), F.lit(0)))
            
            # Calculate derived fields
            .withColumn("gross_amount", 
                       F.round(F.col("quantity") * F.col("unit_price"), 2))
            .withColumn("net_amount",
                       F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
            
            # Data quality flags
            .withColumn("_is_valid", 
                       (F.col("order_id").isNotNull()) & 
                       (F.col("quantity") > 0) &
                       (F.col("unit_price") > 0))
        )
    
    def apply_business_rules(self, df):
        """
        Apply business rule validations
        """
        logger.info("Applying business rules...")
        
        valid_statuses = ["PENDING", "CONFIRMED", "PROCESSING", 
                         "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
        
        return (
            df
            # Filter to valid statuses
            .filter(F.col("order_status").isin(valid_statuses))
            
            # Filter to valid records
            .filter(F.col("_is_valid") == True)
            
            # Remove test orders
            .filter(~F.col("customer_id").startswith("TEST"))
            
            # Ensure reasonable amounts
            .filter(F.col("net_amount") >= 0)
            .filter(F.col("net_amount") <= 100000)  # Max order value
        )
    
    def add_audit_columns(self, df):
        """
        Add audit and metadata columns
        """
        return (
            df
            .withColumn("_silver_timestamp", F.current_timestamp())
            .withColumn("_source_layer", F.lit("bronze"))
            .drop("_is_valid")  # Internal flag not needed downstream
        )
    
    def write_silver(self, df, mode: str = "merge"):
        """
        Write to Silver layer with MERGE for upserts
        
        Args:
            df: Cleaned DataFrame
            mode: 'overwrite', 'append', or 'merge'
        """
        logger.info(f"Writing to Silver: {self.silver_path} (mode={mode})")
        
        if mode == "merge" and DeltaTable.isDeltaTable(self.spark, self.silver_path):
            delta_table = DeltaTable.forPath(self.spark, self.silver_path)
            
            (delta_table.alias("target")
             .merge(
                 df.alias("source"),
                 "target.order_id = source.order_id"
             )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            
        else:
            (df.write
             .format("delta")
             .mode("overwrite" if mode == "overwrite" else "append")
             .partitionBy("order_date")
             .option("overwriteSchema", "true")
             .save(self.silver_path))
    
    def optimize_table(self):
        """
        Optimize Delta table for query performance
        """
        logger.info("Optimizing Silver table...")
        
        delta_table = DeltaTable.forPath(self.spark, self.silver_path)
        
        # Compact small files
        delta_table.optimize().executeCompaction()
        
        # Z-Order by common query columns
        delta_table.optimize().executeZOrderBy("customer_id", "order_date")
        
        # Clean up old versions
        delta_table.vacuum(168)  # 7 days retention
    
    def run(self, incremental: bool = True):
        """
        Execute the Bronze to Silver pipeline
        
        Args:
            incremental: If True, process only new data
        """
        logger.info("=" * 60)
        logger.info("Starting Bronze → Silver Pipeline: Orders")
        logger.info("=" * 60)
        
        # Determine start version for incremental
        start_version = None
        if incremental and DeltaTable.isDeltaTable(self.spark, self.silver_path):
            # Get last processed version from Silver metadata
            history = DeltaTable.forPath(self.spark, self.silver_path).history(1)
            # Logic to track last processed bronze version would go here
            pass
        
        # Read
        bronze_df = self.read_bronze(start_version)
        record_count = bronze_df.count()
        logger.info(f"Read {record_count} records from Bronze")
        
        if record_count == 0:
            logger.info("No new records to process")
            return
        
        # Transform
        deduped = self.deduplicate(bronze_df)
        cleaned = self.clean_data(deduped)
        validated = self.apply_business_rules(cleaned)
        final = self.add_audit_columns(validated)
        
        # Write
        self.write_silver(final, mode="merge" if incremental else "overwrite")
        
        # Optimize periodically
        # self.optimize_table()
        
        final_count = final.count()
        logger.info(f"Wrote {final_count} records to Silver")
        logger.info("Pipeline complete!")


def main():
    """Main entry point"""
    # Initialize Spark with Delta
    spark = (
        SparkSession.builder
        .appName("SilverOrders")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    # Configuration
    bronze_path = "s3://data-lake/bronze/orders"
    silver_path = "s3://data-lake/silver/orders"
    
    # Run pipeline
    cleaner = OrdersCleaner(spark, bronze_path, silver_path)
    cleaner.run(incremental=True)
    
    spark.stop()


if __name__ == "__main__":
    main()
