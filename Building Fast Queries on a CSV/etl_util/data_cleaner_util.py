import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, udf, rank, levenshtein, broadcast, expr, coalesce, lit, when, length, regexp_replace, concat
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

class DataCleanerUtil:
    def __init__(self, spark_session):
        """Initialize with a PySpark DataFrame."""
        self.spark = spark_session

    def drop_nulls(self,df):
        """Drops rows with any null values."""
        df = df.na.drop()
        return df

    def format_with_unit(value, unit):
        """Helper function to format number with a unit, used in UDF."""
        return f"{value}{unit}" if value is not None else None


    def column_has_duplicates(self, df, column_name, enable_printing=False):
        duplicate_counts = df.groupBy(column_name).count().filter(col("count") > 1)
        
        if duplicate_counts.count() > 0:
            if enable_printing:
                print(f"Duplicates found in column '{column_name}': {duplicate_counts.count()}")
            return True
        else:
            if enable_printing:
                print(f"No duplicates found in column '{column_name}'")
            return False


    def clean_company_names(self,df ,valid_companies, threshold=50):
        """
        Apply fuzzy matching to correct company names in the DataFrame using PySpark's built-in Levenshtein function.
        A 50% threshold is tested to have a 100% match based on findings in the Prototype Data Cleaning Section.
        
        The data-cleaning process is as follows:
            1. Create DataFrame for a list of valid companies.
            2. Broadcast the valid company names DataFrame.
            3. Select only invalid company and Calculate Levenshtein distance for each valid company for each invalid company row in the DataFrame.
            4. Filter by best min_distance within each distance based on threshold.
            5. Join back and select the best minimum distance.
            6. Apply threshold and join back the valid company to the original DataFrame.

        Improvements:
            1. Consider adding more df_invalid filters
            2. Add an pre-determined way of counting the minimum distance, for example dela -> dell is near similar
               so it must be they are the same, this way there is no need to check every valid company names.
        """
        # Create a DataFrame from the list of valid company names
        valid_companies_rdd = self.spark.sparkContext.parallelize(valid_companies).map(lambda x: (x,))
        schema = StructType([StructField("Valid_Company", StringType(), True)])
        valid_companies_df = self.spark.createDataFrame(valid_companies_rdd, schema)

        broadcast_valid_companies_df = broadcast(valid_companies_df)

        # Filter out rows where the company name is invalid valid, this is to be fed to the levenshtein method
        df_invalid = df.filter(~col("Company").isin(valid_companies))
        # Filter out rows where the company name is already valid
        df_valid = df.filter(col("Company").isin(valid_companies))

        # Calculate Levenshtein distance for each row using the broadcasted valid companies DataFrame
        company_distances = df_invalid.crossJoin(broadcast_valid_companies_df) \
                                      .withColumn("distance", levenshtein(col("Company"), col("Valid_Company")))

        # Filter for the best match within the threshold
        min_distance_df = company_distances.groupBy("laptop_ID").agg(
                                                    {"distance": "min"}
                                                ).withColumnRenamed("min(distance)", "min_distance")
                                            
        # Rename the `laptop_ID` column in `min_distance_df` to avoid ambiguity
        min_distance_df = min_distance_df.withColumnRenamed("laptop_ID", "laptop_ID_min")

        # Join back to get the closest valid company name
        company_distances = company_distances.join(min_distance_df, 
                                                   (company_distances.laptop_ID == min_distance_df.laptop_ID_min) & 
                                                   (company_distances.distance == min_distance_df.min_distance)) \
                                                        .select("laptop_ID", "Company", "Valid_Company", "distance")

        # Apply threshold to determine if the correction should be applied
        cleaned_df = company_distances.withColumn("Corrected_Company",
                                                  when(col("distance") <= threshold, col("Valid_Company"))
                                                  .otherwise(col("Company")))
        # Select only the necessary columns
        cleaned_laptops = cleaned_df.select("laptop_ID", "Corrected_Company")

        # Combine the valid rows with the cleaned rows
        combined_df = df_valid.union(df.join(cleaned_laptops, "laptop_ID") \
                                     .withColumn("Company", col("Corrected_Company")).drop("Corrected_Company"))
        
        df = combined_df
        return df


    def remove_outliers(self, df):
        """
        Remove outliers on the DataFrame by setting thresholds or ceiling values. If it is higher than or equal to the ceiling value 
        than it is classified as a outlier. It will then be filter out.

        Returns a DataFrame containing no outliers
        """
        # Define thresholds
        max_inches =20
        max_weight = 8  # in kg
        max_ram = 128  
        reasonable_price_range = (50, 150000)  # typical price range in euros
    
        # Fill NaN values in 'Ram' column with '0GB' to avoid errors
        df = df.withColumn('Ram', when(col('Ram').isNull(), '0GB').otherwise(col('Ram')))
    
        # Convert 'Ram' from string with GB to integer for filtering
        df = df.withColumn('Ram', regexp_replace(col('Ram'), 'GB', '').cast(IntegerType()))
    
        # Convert 'Weight' column to numeric values and fill NaN values with 0
        df = df.withColumn('Weight', when(col('Weight').isNull(), '0kg').otherwise(col('Weight')))
        df = df.withColumn('Weight', regexp_replace(col('Weight'), 'kg', '').cast(DoubleType()))
    
        # Filter based on defined thresholds
        df = df.filter(
            (col('Inches') <= max_inches) & 
            (col('Weight') <= max_weight) & 
            (col('Ram') <= max_ram) & 
            (col('Ram') % 4 == 0) &  # Ensuring RAM is a multiple of 4 GB
            (col('Price_euros') >= reasonable_price_range[0]) & 
            (col('Price_euros') <= reasonable_price_range[1])
        )
    
        # Revert the 'Weight' and 'Ram' columns to their original formats
        df = df.withColumn('Weight', concat(col('Weight').cast(StringType()), lit('kg')))
        df = df.withColumn('Ram', concat(col('Ram').cast(StringType()), lit('GB')))
    
        return df

    
    def print_null_counts(self, df):
        """Prints the number of null values for each column in the DataFrame."""
        # Generate an array of column-wise count expressions for null or NaN checks
        null_counts = [count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]
        
        # Compute null counts
        nulls_df = df.select(null_counts).collect()[0]
        
        # Print formatted output for each column
        for c in df.columns:
            print(f"{c}: {nulls_df[c]} null values")


