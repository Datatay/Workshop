from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when
from pyspark.sql.window import Window

#load file
file_path = '/source/bank_transactions.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

#add calculation columns
df = df.withColumn("credit_amount", when(col("transaction_type") == "Credit", col("amount")).otherwise(0)) \
       .withColumn("debit_amount", when(col("transaction_type") == "Debit", col("amount")).otherwise(0))

#calculate balances
windowSpec = Window.partitionBy("account_number").orderBy("TransactionDate", "account_number") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("running_balance", spark_sum(col("credit_amount") - col("debit_amount")).over(windowSpec))

#final select
df = df.select(
    col("TransactionDate"),
    col("account_number").alias("AccountNumber"),
    col("transaction_type").alias("TransactionType"),
    col("amount").alias("Amount"),
    col("running_balance").alias("CurrentBalance")
    )
df.show()