import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CustomerOrderJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Customer Order Join")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // =====================================================
    // CUSTOMER DATA
    // =====================================================

    val customers = Seq(
      (1, "Chintan", "Hyderabad"),
      (2, "Rahul", "Mumbai"),
      (3, "Priya", "Bangalore"),
      (4, "Amit", "Delhi"),
      (5, "Kiran", "Chennai")
    ).toDF(
      "customer_id",
      "customer_name",
      "city"
    )

    // =====================================================
    // ORDER DATA
    // =====================================================

    val orders = Seq(
      (101, 1, 50000),
      (102, 2, 20000),
      (103, 1, 25000),
      (104, 3, 30000),
      (105, 4, 15000),
      (106, 2, 40000),
      (107, 6, 35000)
    ).toDF(
      "order_id",
      "customer_id",
      "amount"
    )

    // =====================================================
    // DISPLAY CUSTOMER DATA
    // =====================================================

    println("===== CUSTOMERS =====")
    customers.show()

    // =====================================================
    // DISPLAY ORDER DATA
    // =====================================================

    println("===== ORDERS =====")
    orders.show()

    // =====================================================
    // INNER JOIN
    // =====================================================

    println("===== INNER JOIN =====")

    val innerJoin = customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "inner"
      )
      .select(
        customers("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("amount")
      )

    innerJoin.show()

    // =====================================================
    // LEFT JOIN
    // =====================================================

    println("===== LEFT JOIN =====")

    val leftJoin = customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "left"
      )
      .select(
        customers("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("amount")
      )

    leftJoin.show()

    // =====================================================
    // RIGHT JOIN
    // =====================================================

    println("===== RIGHT JOIN =====")

    val rightJoin = customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "right"
      )
      .select(
        customers("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("amount")
      )

    rightJoin.show()

    // =====================================================
    // CUSTOMER TOTAL SPENDING
    // =====================================================

    println("===== CUSTOMER TOTAL SPENDING =====")

    val customerTotal = innerJoin
      .groupBy(
        "customer_id",
        "customer_name",
        "city"
      )
      .agg(
        sum("amount").alias("total_spending"),
        count("order_id").alias("order_count")
      )
      .orderBy(desc("total_spending"))

    customerTotal.show()

    // =====================================================
    // ORDER RANKING
    // =====================================================

    println("===== ORDER RANKING =====")

    val windowSpec =
      Window
        .partitionBy("customer_id")
        .orderBy(desc("amount"))

    val rankedOrders = innerJoin
      .withColumn(
        "order_rank",
        row_number().over(windowSpec)
      )
      .orderBy(
        "customer_id",
        "order_rank"
      )

    rankedOrders.show()

    // =====================================================
    // FINAL RESULT
    // =====================================================

    println("===== FINAL RESULT =====")

    val finalResult = innerJoin
      .withColumn(
        "total_spending",
        sum("amount").over(
          Window.partitionBy("customer_id")
        )
      )
      .withColumn(
        "order_rank",
        row_number().over(windowSpec)
      )
      .orderBy(
        "customer_id",
        "order_rank"
      )

    finalResult.show()

    spark.stop()
  }
}
