### Custom DataFusion Function Implementation

This is a custom function implementation for DataFusion in rust.
This library implements the 'Greatest' function as is used by the 'pyspark.sql.functions.greatest' library as an example.

The code contains the library implementation, tests for the library and the binary to load csv file and use the library's registration hook.

### ColumnVar:
The library works with Columnvar Types:

If the Columnvar array is defined with `vec![9.0, 5.0, 9.0]`, this means:

```rust
//Column_1 (3x1):
[
9.0
5.0
9.0
]
```

### Float32Array Example:
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
