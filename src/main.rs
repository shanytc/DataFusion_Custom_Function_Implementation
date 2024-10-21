use datafusion_greatest::{register_greatest_function};
use datafusion::logical_expr::{col};
use datafusion::execution::FunctionRegistry;
use datafusion::prelude::{CsvReadOptions, SessionContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    if let Err(e) = register_greatest_function(&ctx, false) {
        panic!("Failed to register 'greatest' function: {}", e);
    }

    // Create a dataframe similar to the Spark example
    let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?;

    // Get the UDF from the context
    let udf = ctx.udf("greatest").expect("Error getting UDF");

    // Test greatest function with column references
    let result = df.select(vec![
        udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
    ])?
        .collect()
        .await
        .expect("Error executing query");


    println!("Greatest values: {:?}", result);
    Ok(())
}
