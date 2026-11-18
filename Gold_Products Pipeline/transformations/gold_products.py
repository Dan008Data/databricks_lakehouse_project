
from pyspark.sql.functions import *
from pyspark import pipelines as dp

#Expectations

my_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "product_name IS NOT NULL"
    }

# 1. Create the staging streaming table # Read incoming data and apply expectations
@dp.expect_all_or_drop(my_rules)

@dp.table(name="DimProducts_stage")
def DimProducts_stage():
    return spark.readStream.table(
        "databricks_cata.silver.products_silver"
    )

# 2. Create a temporary view
@dp.temporary_view(name="DimProducts_view")
def DimProducts_view():
    return spark.readStream.table("DimProducts_stage")


# 3. Create the target streaming table
dp.create_streaming_table(
    name="DimProducts"
)


# 4. Apply CDC
dp.create_auto_cdc_flow(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    sequence_by=col("product_id"),
    stored_as_scd_type=2
)
