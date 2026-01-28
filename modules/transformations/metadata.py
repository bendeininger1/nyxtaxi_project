from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Adds a 'processed_timestamp' column to a DataFrame with the current  timestamp.

    Parameters:
        df (Dataframe): The input Spark DataFrame.

    Returns:
        DataFrame: The DataFrame with an additional 'processed_timestamp'.
    """
    return df.withColumn("processed_timestamp", current_timestamp())