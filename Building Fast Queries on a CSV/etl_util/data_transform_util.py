import pyspark
from pyspark.sql.functions import  regexp_extract, regexp_replace, split, expr, when, col, trim, round

class DataTransformUtil:
    def __init__(self, spark):
        self.spark = spark
    
    def split_screen_resolution(self, df):
        """
        Splits the 'ScreenResolution' into 'DisplayType' and 'ScreenSize',
        handling cases where the split might result in empty DisplayType.
        """
        # Splitting the ScreenResolution at the last space to separate DisplayType and ScreenSize
        df = df.withColumn("ScreenSize", expr("split(ScreenResolution, ' ')[size(split(ScreenResolution, ' ')) - 1]"))
        df = df.withColumn("DisplayType", expr("substring(ScreenResolution, 1, length(ScreenResolution) - length(ScreenSize) - 1)"))

        # Handling empty DisplayType cases by setting them to 'Unknown'
        df = df.withColumn("DisplayType", trim(col("DisplayType")))
        df= df.withColumn("DisplayType", when(col("DisplayType") == "", "Unknown").otherwise(col("DisplayType")))

        df = df.drop("ScreenResolution")
        return df

    def split_cpu(self, df):
        # Captures the first word, which is brand
        df = df.withColumn("CpuBrand", regexp_extract("Cpu", r"(\w+)", 1)) 
        # Remove Brand at the start and Speed GHz at the end
        df= df.withColumn("CpuModel", regexp_replace("Cpu", r"^\S+\s+|\s+\d+\.?\d*\s*[gG][hH][zZ]", ""))
        # Captures the speed, which is usually has Ghz at its end
        df = df.withColumn("CpuSpeed", regexp_extract("Cpu", r"(\d+\.?\d*\s*[gG][hH][zZ])", 0))
        df = df.drop("Cpu")
        return df



    def split_gpu(self, df):
        # Extract first word (brand) from 'Gpu' column
        df = df.withColumn('GpuBrand', regexp_extract('Gpu', r'(^[\w]+)', 1))
        
        # Rename 'Gpu' column to 'GpuModel'
        df = df.withColumnRenamed("Gpu", "GpuModel")
        
        # Remove the first word (brand) and the following space from 'GpuModel' column
        df = df.withColumn("GpuModel", regexp_replace("GpuModel", r'^[\w]+\s?', ""))
        
        return df

    def weight_to_weight_kg(self, df):
        df = df.withColumn('WeightKG', round(regexp_extract('Weight', r'(\d+.\d+)', 1).cast('float'), 2))
        df = df.drop("Weight")
        
        return df

    def create_cpu_dataframe(self, df):
        cpu_df = df.select("CpuBrand", "CpuModel", "CpuSpeed").distinct()
        
        # Generate a unique ID for each CPU
        cpu_df = cpu_df.withColumn("cpu_id", row_number().over(Window.orderBy("CpuBrand", "CpuModel", "CpuSpeed")))
        return cpu_df


        

