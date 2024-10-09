import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when

def scrape_data():
    url = "http://books.toscrape.com/catalogue/category/books_1/index.html"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from {url}")
    soup = BeautifulSoup(response.text, "html.parser")
    data = []
    books = soup.find_all("article", class_="product_pod")
    for book in books:
        title = book.h3.a["title"]
        price = book.find("p", class_="price_color").text.strip()
        rating = book.p["class"][1]
        data.append((title, price, rating))
    return data

def run_spark_job():
    spark = SparkSession.builder \
        .appName("Spark Web Scraper - Books") \
        .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    scraped_data = scrape_data()
    columns = ["title", "price", "rating"]
    spark_df = spark.createDataFrame(scraped_data, schema=columns)
    spark_df = spark_df.withColumn("price", regexp_replace(col("price"), "£", "").cast("float"))
    spark_df = spark_df.withColumn("price_category",
                                   when(col("price") < 20, "cheap")
                                   .when((col("price") >= 20) & (col("price") < 35), "moderate")
                                   .otherwise("expensive"))
    sorted_df = spark_df.orderBy(col("price").asc())
    sorted_df.show(truncate=False)
    sorted_df.write.csv("../data/output_books.csv", header=True)
    spark.stop()

if __name__ == "__main__":
    run_spark_job()
