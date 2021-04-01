import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def transform_date(date_str):
    return F.when(F.to_date(date_str, "dd MMM yyyy").isNotNull(), F.to_date(date_str, "dd MMM yyyy")) \
        .when(F.to_date(date_str, "dd/MM/yyyy").isNotNull(), F.to_date(date_str, "dd/MM/yyyy")) \
        .when(F.to_date(date_str, "yyyy").isNotNull(), F.to_date(date_str, "yyyy")) \
        .otherwise("Unknown Format")

def transform_address(addr_str):
    return addr_str.cast("string")
def transform_id_numbers(id_no_str):
    return id_no_str.cast("string")

def main():
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    gbr_df = spark.read.json("gbr.jsonl")
    ofac_df = spark.read.json("ofac.jsonl")

    ofac_df_trans = ofac_df \
        .withColumn("reported_dates_of_birth", F.transform(ofac_df.reported_dates_of_birth, transform_date)) \
        .withColumn("addresses", F.transform(ofac_df.addresses, transform_address)) \
        .withColumn("id_numbers", F.transform(ofac_df.id_numbers, transform_id_numbers))
    
    gbr_df_trans = gbr_df \
        .withColumn("reported_dates_of_birth", F.transform(gbr_df.reported_dates_of_birth, transform_date)) \
        .withColumn("addresses", F.transform(gbr_df.addresses, transform_address)) \
        .withColumn("id_numbers", F.transform(gbr_df.id_numbers, transform_id_numbers))
    
    match_names = ofac_df_trans \
        .join(gbr_df_trans, [
            ofac_df_trans.name == gbr_df_trans.name, \
            ofac_df_trans.type == gbr_df_trans.type, \
            F.arrays_overlap(ofac_df_trans.reported_dates_of_birth, gbr_df_trans.reported_dates_of_birth)
        ]) \
        .select( \
            ofac_df_trans.id.alias("ofac_id"), \
            gbr_df_trans.id.alias("uk_id"), \
            ofac_df_trans.name, \
            ofac_df_trans.type, \
            F.array_intersect(ofac_df_trans.reported_dates_of_birth, gbr_df_trans.reported_dates_of_birth).alias("dates_of_birth") \
        )
    
    match_ids = ofac_df \
        .join(gbr_df, [
            F.arrays_overlap(ofac_df.id_numbers.value, gbr_df.id_numbers.value), \
            ofac_df.type == gbr_df.type \
        ]).select( \
            ofac_df.id.alias("ofac_id"), \
            gbr_df.id.alias("uk_id"), \
            ofac_df.name, \
            ofac_df.type, \
            F.array_intersect(ofac_df.id_numbers.value, gbr_df.id_numbers.value).alias("id_numbers") \
        )
    
    matches_aliases_1 = ofac_df \
        .join(gbr_df, [ \
            F.array_contains(ofac_df.aliases.value, gbr_df.name), \
            ofac_df.type == gbr_df.type \
        ]).select( \
            ofac_df.id.alias("ofac_id"), \
            gbr_df.id.alias("uk_id"), \
            ofac_df.name, \
            ofac_df.type, \
            ofac_df.aliases, \
        )
    matches_aliases_2 = gbr_df \
        .join(ofac_df, [ \
            F.array_contains(gbr_df.aliases.value, ofac_df.name), \
            ofac_df.type == gbr_df.type \
        ]).select( \
            ofac_df.id.alias("ofac_id"), \
            gbr_df.id.alias("uk_id"), \
            gbr_df.name, \
            gbr_df.type, \
            gbr_df.aliases, \
        )
    
    matches_aliases = matches_aliases_1.union(matches_aliases_2)

    match_names.coalesce(1).write.json("results", mode="overwrite")
    match_ids.coalesce(1).write.json("results", mode="append")
    matches_aliases.coalesce(1).write.json("results", mode="append")

if __name__ == "__main__":
    main()