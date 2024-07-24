from pyspark.sql import DataFrame, SparkSession


spark = SparkSession.builder.appName("tst").getOrCreate()

category = spark.createDataFrame([
        {"id": 1, "catName": "book"},
        {"id": 2, "catName": "pen"},
        {"id": 3, "catName": "camera"},
])

product = spark.createDataFrame([
        {"prodId": 1, "catId": 1, "prodName": "W&M"},
        {"prodId": 2, "catId": 3, "prodName": "Blue"},
        {"prodId": 3, "catId": 1, "prodName": "Ozark"},
        {"prodId": 4, "catId": 2, "prodName": "Red"},
        {"prodId": 5, "catId": 3, "prodName": "Sony"},
        {"prodId": 6, "catId": 1, "prodName": "Sony"},
        {"prodId": 7, "catId": 2, "prodName": "Sony"},
        {"prodId": 8, "catId": 3, "prodName": "Ozark"},
        {"prodId": 9, "catId": None, "prodName": "Pank"},
        {"prodId": 10, "catId": None, "prodName": "Kanp"}
])


res_method = (product.join(category, product['catId'] == category['id'], 'full_outer')
              .select(product['prodName'].alias('Product'), category['catName'].alias('Category'))
              )

res_method.show()

