```
pub struct DeltaOps(pub DeltaTable);
```

Expand description

The deltalake crate is currently just a meta-package shim for deltalake-core High level interface for executing commands against a DeltaTable

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#127)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-DeltaOps)

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#137)

Create a new [`DeltaOps`](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html "struct deltalake::DeltaOps") instance, operating on [`DeltaTable`](https://docs.rs/deltalake/latest/deltalake/struct.DeltaTable.html "struct deltalake::DeltaTable") at given uri.

```
use deltalake_core::DeltaOps;

async {
    let ops = DeltaOps::try_from_uri("memory:///").await.unwrap();
};
```

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#148-151)

try from uri with storage options

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#174)

Create a new [`DeltaOps`](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html "struct deltalake::DeltaOps") instance, backed by an un-initialized in memory table

Using this will not persist any changes beyond the lifetime of the table object. The main purpose of in-memory tables is for use in testing.

```
use deltalake_core::DeltaOps;

let ops = DeltaOps::new_in_memory();
```

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#193)

Create a new Delta table

```
use deltalake_core::DeltaOps;

async {
    let ops = DeltaOps::try_from_uri("memory:///").await.unwrap();
    let table = ops.create().with_table_name("my_table").await.unwrap();
    assert_eq!(table.version(), Some(0));
};
```

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#200)

Load data from a DeltaTable

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#207)

Load a table with CDF Enabled

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#214)

Write data to Delta table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#220)

Vacuum stale files from delta table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#226)

Audit active files with files present on the filesystem

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#233)

Audit active files with files present on the filesystem

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#240)

Delete data from Delta table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#247)

Update data from Delta table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#253)

Restore delta table to a specified version or datetime

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#260-264)

Update data from Delta table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#276)

Add a check constraint to a table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#282)

Enable a table feature for a table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#289)

Drops constraints from a table

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#294)

Set table properties

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#299)

Add new columns

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#304)

Update field metadata

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#309)

Update table metadata

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#326)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-AsRef%3CDeltaTable%3E-for-DeltaOps)

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#327)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#method.as_ref)

Converts this type into a shared reference of the (usually inferred) input type.

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#320)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-From%3CDeltaOps%3E-for-DeltaTable)

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#321)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#method.from-1)

Converts to this type from the input type.

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#314)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-From%3CDeltaTable%3E-for-DeltaOps)

[Source](https://docs.rs/deltalake-core/0.27.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html#315)[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#method.from)

Converts to this type from the input type.

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-Freeze-for-DeltaOps)

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-RefUnwindSafe-for-DeltaOps)

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-Send-for-DeltaOps)

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-Sync-for-DeltaOps)

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-Unpin-for-DeltaOps)

[§](https://docs.rs/deltalake/latest/deltalake/struct.DeltaOps.html#impl-UnwindSafe-for-DeltaOps)