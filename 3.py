from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("Name", StringType()),
    StructField("Age", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Occupation", StringType()),
    StructField("Hobby", StringType()),
])

df = spark.read.format('csv').schema(schema).load('3_data.csv')

df.write.option("header",True).csv('3_answer.csv')
print('file saved as "3_answer.csv"')

#add a row number column to help identify row numbers with nulls
w = Window().orderBy(lit('A'))
df = df.withColumn("row_num", row_number().over(w))

null_rows = (df
             .select('row_num')
             .filter(df.Name.isNull() | df.Age.isNull() | df.Gender.isNull()| df.Occupation.isNull())
             .collect()
            )

print("there are nulls in the following rows:")
for row in null_rows:
    print(row.row_num)