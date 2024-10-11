use std::any::Any;
use std::sync::Arc;
use arrow::array::{Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::prelude::{SessionContext};

trait Greatest<T> {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<T>>;
}

impl Greatest<i32> for Int32Array {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<i32>> {
        let mut result = vec![];

        // Get the number of rows to process by checking the length of the first array
        let num_rows = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => return Err(DataFusionError::Internal("Invalid argument type".to_string())),
        };

        for i in 0..num_rows {
            let mut max_value: Option<i32> = None;

            for arg in args {
                match arg {
                    ColumnarValue::Array(arr) => {
                        let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                        let current_value = if int_arr.is_null(i) { 0 } else { int_arr.value(i) };
                        max_value = Some(max_value.map_or(current_value, |max_val| max_val.max(current_value)));
                    }
                    _ => return Err(DataFusionError::Internal("Unexpected argument type".to_string())),
                }
            }
            result.push(max_value.unwrap_or_default());
        }

        Ok(result)
    }
}

impl Greatest<i64> for Int64Array {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<i64>> {
        let mut result = vec![];

        // Get the number of rows to process by checking the length of the first array
        let num_rows = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => return Err(DataFusionError::Internal("Invalid argument type".to_string())),
        };

        for i in 0..num_rows {
            let mut max_value: Option<i64> = None;

            for arg in args {
                match arg {
                    ColumnarValue::Array(arr) => {
                        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                        let current_value = if int_arr.is_null(i) { 0 } else { int_arr.value(i) };
                        max_value = Some(max_value.map_or(current_value, |max_val| max_val.max(current_value)));
                    }
                    _ => return Err(DataFusionError::Internal("Unexpected argument type".to_string())),
                }
            }
            result.push(max_value.unwrap_or_default());
        }

        Ok(result)
    }
}

impl Greatest<f32> for Float32Array {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<f32>> {
        let mut result = vec![];

        // Get the number of rows to process by checking the length of the first array
        let num_rows = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => return Err(DataFusionError::Internal("Invalid argument type".to_string())),
        };

        for i in 0..num_rows {
            let mut max_value: Option<f32> = None;

            for arg in args {
                match arg {
                    ColumnarValue::Array(arr) => {
                        let float_arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
                        let current_value = if float_arr.is_null(i) { 0.0 } else { float_arr.value(i) };
                        max_value = Some(max_value.map_or(current_value, |max_val| max_val.max(current_value)));
                    }
                    _ => return Err(DataFusionError::Internal("Unexpected argument type".to_string())),
                }
            }
            result.push(max_value.unwrap_or_default());
        }

        Ok(result)
    }
}

impl Greatest<f64> for Float64Array {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<f64>> {
        let mut result = vec![];

        // Get the number of rows to process by checking the length of the first array
        let num_rows = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => return Err(DataFusionError::Internal("Invalid argument type".to_string())),
        };

        for i in 0..num_rows {
            let mut max_value: Option<f64> = None;

            for arg in args {
                match arg {
                    ColumnarValue::Array(arr) => {
                        let float_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                        let current_value = if float_arr.is_null(i) { 0.0 } else { float_arr.value(i) };
                        max_value = Some(max_value.map_or(current_value, |max_val| max_val.max(current_value)));
                    }
                    _ => return Err(DataFusionError::Internal("Unexpected argument type".to_string())),
                }
            }
            result.push(max_value.unwrap_or_default());
        }

        Ok(result)
    }
}

impl Greatest<String> for StringArray {
    fn greatest_value(args: &[ColumnarValue]) -> Result<Vec<String>> {
        let mut result = vec![];

        // Get the number of rows to process by checking the length of the first array
        let num_rows = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => return Err(DataFusionError::Internal("Invalid argument type".to_string())),
        };

        for i in 0..num_rows {
            let mut max_value: Option<String> = None;

            for arg in args {
                match arg {
                    ColumnarValue::Array(arr) => {
                        let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                        let current_value = if string_arr.is_null(i) {
                            "".to_string() // Treat null as an empty string
                        } else {
                            string_arr.value(i).to_string() // Get the string value
                        };

                        max_value = Some(max_value.map_or(current_value.clone(), |max_val| {
                            if max_val >= current_value {
                                max_val
                            } else {
                                current_value.clone()
                            }
                        }));
                    }
                    _ => return Err(DataFusionError::Internal("Unexpected argument type".to_string())),
                }
            }
            result.push(max_value.unwrap_or_default());
        }

        Ok(result)
    }
}


#[derive(Debug)]
struct GreatestFunction {
    signature: Signature,
}

impl GreatestFunction {
    fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Variadic(vec![DataType::Int32]),
                TypeSignature::Variadic(vec![DataType::Int64]),
                TypeSignature::Variadic(vec![DataType::Float32]),
                TypeSignature::Variadic(vec![DataType::Float64]),
                TypeSignature::Variadic(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for GreatestFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        if _args.is_empty() {
            return Err(DataFusionError::Internal(
                "greatest function requires at least one argument".to_string(),
            ));
        }
        Ok(_args[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return Err(DataFusionError::Internal(
                "greatest function requires at least one argument".to_string(),
            ));
        }

        match &args[0] {
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Int32 => greatest_value_gen::<i32, Int32Array>(args),
                DataType::Int64 => greatest_value_gen::<i64, Int64Array>(args),
                DataType::Float32 => greatest_value_gen::<f32, Float32Array>(args),
                DataType::Float64 => greatest_value_gen::<f64, Float64Array>(args),
                DataType::Utf8 => greatest_value_gen::<String, StringArray>(args),
                _ => Err(DataFusionError::Internal(
                    format!("Unsupported data type for greatest function: {:?}", arr.data_type())
                )),
            },
            ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                "greatest function does not support scalar values".to_string(),
            )),
        }
    }
}

// register the greatest function inside the DataFusion context
pub fn register_greatest_function(ctx: &SessionContext) -> Result<()> {
    let greatest_fn = ScalarUDF::from(GreatestFunction::new());
    ctx.register_udf(greatest_fn);
    Ok(())
}

// generic function to calculate the greatest value for a given type
fn greatest_value_gen<T, A>(args: &[ColumnarValue]) -> Result<ColumnarValue>
where
    A: Greatest<T> + 'static + Array + From<Vec<T>>,
    T: Default,
{
    let result = A::greatest_value(args)?;
    let array_ref = Arc::new(A::from(result));
    Ok(ColumnarValue::Array(array_ref))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{Field, Schema};
    use datafusion::assert_batches_eq;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::{col};
    use super::*;

    #[tokio::test]
    async fn test_greatest_empty_arr_function() -> Result<()> {
        let ctx = SessionContext::new();
        if let Err(e) = register_greatest_function(&ctx) {
            panic!("Failed to register 'greatest' function: {}", e);
        }

        // Create a schema for our data
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));

        // Create data
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None, None, None])),
                Arc::new(Int32Array::from(vec![None, None, None])),
                Arc::new(Int32Array::from(vec![None, None, None])),
            ],
        ).expect("Error creating RecordBatch");

        // Create a DataFrame
        let df = ctx.read_batch(data).expect("Error creating DataFrame");

        // Get the UDF from the context
        let udf = ctx.udf("greatest").expect("Error getting UDF");

        // Test greatest function with column references
        let result = df.select(vec![
            udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
        ])?
            .collect()
            .await
            .expect("Error executing query");

        assert_batches_eq!(
            &[
                "+----------+",
                "| greatest |",
                "+----------+",
                "| 0        |",
                "| 0        |",
                "| 0        |",
                "+----------+",
            ],
            &result
        );

        Ok(())

    }

    #[tokio::test]
    async fn test_greatest_int32_function() -> Result<()> {
        let ctx = SessionContext::new();
        if let Err(e) = register_greatest_function(&ctx) {
            panic!("Failed to register 'greatest' function: {}", e);
        }

        // Create a schema for our data
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));

        // Create data
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(9), Some(2), Some(3)])),
                Arc::new(Int32Array::from(vec![None, Some(5), Some(6)])),
                Arc::new(Int32Array::from(vec![Some(3), Some(2), Some(9)])),
            ],
        ).expect("Error creating RecordBatch");

        // Create a DataFrame
        let df = ctx.read_batch(data).expect("Error creating DataFrame");

        // Get the UDF from the context
        let udf = ctx.udf("greatest").expect("Error getting UDF");

        // Test greatest function with column references
        let result = df.select(vec![
            udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
        ])?
        .collect()
        .await
        .expect("Error executing query");

        assert_batches_eq!(
            &[
                "+----------+",
                "| greatest |",
                "+----------+",
                "| 9        |",
                "| 5        |",
                "| 9        |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_greatest_int64_function() -> Result<()> {
        let ctx = SessionContext::new();
        if let Err(e) = register_greatest_function(&ctx) {
            panic!("Failed to register 'greatest' function: {}", e);
        }

        // Create a schema for our data
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        // Create data
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(9), Some(2), Some(3), Some(10)])),
                Arc::new(Int64Array::from(vec![None, Some(5), Some(6), Some(8)])),
                Arc::new(Int64Array::from(vec![Some(3), Some(2), Some(9), Some(11)])),
            ],
        ).expect("Error creating RecordBatch");

        // Create a DataFrame
        let df = ctx.read_batch(data).expect("Error creating DataFrame");

        // Get the UDF from the context
        let udf = ctx.udf("greatest").expect("Error getting UDF");

        // Test greatest function with column references
        let result = df.select(vec![
            udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
        ])?
            .collect()
            .await
            .expect("Error executing query");

        assert_batches_eq!(
            &[
                "+----------+",
                "| greatest |",
                "+----------+",
                "| 9        |",
                "| 5        |",
                "| 9        |",
                "| 11       |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_greatest_float32_function() -> Result<()> {
        let ctx = SessionContext::new();
        if let Err(e) = register_greatest_function(&ctx) {
            panic!("Failed to register 'greatest' function: {}", e);
        }

        // Create a schema for our data
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
            Field::new("c", DataType::Float32, true),
        ]));

        // Create data
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![Some(9.0), Some(2.0), Some(3.0)])),
                Arc::new(Float32Array::from(vec![None, Some(5.0), Some(6.0)])),
                Arc::new(Float32Array::from(vec![Some(3.0), Some(2.0), Some(9.0)])),
            ],
        ).expect("Error creating RecordBatch");

        // Create a DataFrame
        let df = ctx.read_batch(data).expect("Error creating DataFrame");

        // Get the UDF from the context
        let udf = ctx.udf("greatest").expect("Error getting UDF");

        // Test greatest function with column references
        let result = df.select(vec![
            udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
        ])?
            .collect()
            .await
            .expect("Error executing query");

        assert_batches_eq!(
            &[
                "+----------+",
                "| greatest |",
                "+----------+",
                "| 9.0      |",
                "| 5.0      |",
                "| 9.0      |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_greatest_float64_function() -> Result<()> {
        let ctx = SessionContext::new();
        if let Err(e) = register_greatest_function(&ctx) {
            panic!("Failed to register 'greatest' function: {}", e);
        }

        // Create a schema for our data
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Float64, true),
        ]));

        // Create data
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![Some(9.0), Some(2.0), Some(3.0)])),
                Arc::new(Float64Array::from(vec![None, Some(5.0), Some(6.0)])),
                Arc::new(Float64Array::from(vec![Some(3.0), Some(2.0), Some(9.0)])),
            ],
        ).expect("Error creating RecordBatch");

        // Create a DataFrame
        let df = ctx.read_batch(data).expect("Error creating DataFrame");

        // Get the UDF from the context
        let udf = ctx.udf("greatest").expect("Error getting UDF");

        // Test greatest function with column references
        let result = df.select(vec![
            udf.call(vec![col("a"), col("b"), col("c")]).alias("greatest"),
        ])?
            .collect()
            .await
            .expect("Error executing query");

        assert_batches_eq!(
            &[
                "+----------+",
                "| greatest |",
                "+----------+",
                "| 9.0      |",
                "| 5.0      |",
                "| 9.0      |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }

    #[test]
    fn test_int32_greatest_function_direct() {
        // Create test arguments as Int32 arrays
        let int32_args = vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1), Some(3), Some(2)]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(4)]))),
        ];

        // Initialize the Greatest function
        let greatest_fn = GreatestFunction::new();

        // Call the invoke method on the greatest function and handle errors
        let result = greatest_fn.invoke(&int32_args).expect("Failed to invoke greatest function");

        // Check if the result is an array and downcast it to Int32Array
        if let ColumnarValue::Array(arr) = result {
            let int_arr = arr.as_any().downcast_ref::<Int32Array>()
                .expect("Failed to downcast result to Int32Array");

            // Assert the result matches the expected output
            assert_eq!(int_arr, &Int32Array::from(vec![2, 3, 4]));
        } else {
            panic!("Expected Array result, got something else");
        }
    }

    #[test]
    fn test_int64_greatest_function_direct() {
        // Create test arguments as Int32 arrays
        let int64_args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(1), Some(3), Some(2)]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(4)]))),
        ];

        // Initialize the Greatest function
        let greatest_fn = GreatestFunction::new();

        // Call the invoke method on the greatest function and handle errors
        let result = greatest_fn.invoke(&int64_args).expect("Failed to invoke greatest function");

        // Check if the result is an array and downcast it to Int64Array
        if let ColumnarValue::Array(arr) = result {
            let int_arr = arr.as_any().downcast_ref::<Int64Array>()
                .expect("Failed to downcast result to Int64Array");

            // Assert the result matches the expected output
            assert_eq!(int_arr, &Int64Array::from(vec![2, 3, 4]));
        } else {
            panic!("Expected Array result, got something else");
        }
    }

    #[test]
    fn test_float32_greatest_function_direct() {
        // Create test arguments as Float32 arrays
        let float32_args = vec![
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![Some(1.0), Some(3.0), Some(2.0)]))),
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![Some(2.0), Some(1.0), Some(4.0)]))),
        ];

        // Initialize the Greatest function
        let greatest_fn = GreatestFunction::new();

        // Call the invoke method on the greatest function and handle errors
        let result = greatest_fn.invoke(&float32_args).expect("Failed to invoke greatest function");

        // Check if the result is an array and downcast it to Float32Array
        if let ColumnarValue::Array(arr) = result {
            let int_arr = arr.as_any().downcast_ref::<Float32Array>()
                .expect("Failed to downcast result to Float32Array");

            // Assert the result matches the expected output
            assert_eq!(int_arr, &Float32Array::from(vec![2.0, 3.0, 4.0]));
        } else {
            panic!("Expected Array result, got something else");
        }
    }

    #[test]
    fn test_float64_greatest_function_direct() {
        // Create test arguments as Float32 arrays
        let float64_args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0), Some(2.0)]))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(2.0), Some(1.0), Some(4.0)]))),
        ];

        // Initialize the Greatest function
        let greatest_fn = GreatestFunction::new();

        // Call the invoke method on the greatest function and handle errors
        let result = greatest_fn.invoke(&float64_args).expect("Failed to invoke greatest function");

        // Check if the result is an array and downcast it to Floa64Array
        if let ColumnarValue::Array(arr) = result {
            let int_arr = arr.as_any().downcast_ref::<Float64Array>()
                .expect("Failed to downcast result to Float64Array");

            // Assert the result matches the expected output
            assert_eq!(int_arr, &Float64Array::from(vec![2.0, 3.0, 4.0]));
        } else {
            panic!("Expected Array result, got something else");
        }
    }

    #[test]
    fn test_string_greatest_function_direct() {
        // Create test arguments as StringArray
        let string_args = vec![
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("banana"), Some("coffee"), Some("apple")]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("peach"), Some("monkey"), Some("game")]))),
        ];

        // Initialize the Greatest function
        let greatest_fn = GreatestFunction::new();

        // Call the invoke method on the greatest function and handle errors
        let result = greatest_fn.invoke(&string_args).expect("Failed to invoke greatest function");

        // Check if the result is an array and downcast it to StringArray
        if let ColumnarValue::Array(arr) = result {
            let int_arr = arr.as_any().downcast_ref::<StringArray>()
                .expect("Failed to downcast result to StringArray");

            // Assert the result matches the expected output
            assert_eq!(int_arr, &StringArray::from(vec!["peach", "monkey", "game"]));
        } else {
            panic!("Expected Array result, got something else");
        }
    }
}
