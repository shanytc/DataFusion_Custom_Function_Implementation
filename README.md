## Custom DataFusion Function Implementation via UDF

This is a custom function implementation (UDF) for DataFusion using Rust programming language.
Our library implements the `Greatest` function as is used by the [pyspark.sql.functions.greatest](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.greatest.html) library as an example.

The code contains the library implementation, tests for the library and the binary to load csv file and use the library's registration hook.

### Session Registration
In order to add our custom function to DataFusion, we shall first create a session and than register our custom function with that context.

```rust
let ctx = SessionContext::new();
if let Err(e) = register_greatest_function(&ctx, false) {
    panic!("Failed to register 'greatest' function: {}", e);
}
```

### Access UDF (User Defined Function)
After registering function, we can get an handle to it using the context's udf() function.
```rust
// Get the UDF from the context
let udf = ctx.udf("greatest").expect("Error getting UDF");
```

### Schemas
Sometimes it's easier to work with schemas when we create out data-frame.
```rust
 // Create a schema for our data
let schema = Arc::new(Schema::new(vec![
    Field::new("a", DataType::Int32, true),
    Field::new("b", DataType::Int32, true),
    Field::new("c", DataType::Int32, true),
]));
```

### ColumnVar:
The library works with ColumnVar Types:

If the Columnvar array is defined with `vec![9.0, 5.0, 9.0]`, this means:

```rust
//Column_1 (3x1):
[
9.0
5.0
9.0
]
```

### Float32Array Example with Schema attached:
```rust
 let data = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(Float32Array::from(vec![Some(9.0), Some(2.0), Some(3.0)])),
        Arc::new(Float32Array::from(vec![None, Some(5.0), Some(6.0)])),
        Arc::new(Float32Array::from(vec![Some(3.0), Some(2.0), Some(9.0)])),
    ],
).expect("Error creating RecordBatch");
```
we will get 

| Column 1 (Float32) | Column 2 (Float32) | Column 3 (Float32) |
|--------------------|--------------------|--------------------|
| 9.0                | NULL               | 3.0                |
| 2.0                | 5.0                | 2.0                |
| 3.0                | 6.0                | 9.0                |

the algorithm iterates rows and columns (`O(n*m)`).
each column is sent as an argument that to the `greatest` trait implementation (based on it's data-type) and finds the maximum value per each row.

- Created by Shany Golan
