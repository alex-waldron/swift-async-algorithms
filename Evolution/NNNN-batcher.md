# Batcher

An `AsyncSequence` that batches values and yields them transactionally on demand.

## Key Characteristics

**Transactional Batching**
Values accumulate until the next suspension point, similar to how `@Observable` batches mutations. All values sent between suspension points are yielded as a single array.

**Natural Backpressure**
When iteration is slow, multiple `send()` calls automatically batch together. No explicit backpressure API needed—the iterator's processing speed naturally determines batch boundaries.

**Non-Blocking Send**
`send()` never blocks or suspends. Fire-and-forget from any context.

**Single Consumer**
Supports one active iteration. Safe for concurrent sends from multiple tasks.

## Usage

```swift
let batcher = Batcher<String>()

// Producer: send from anywhere, never blocks
Task {
    batcher.send("First")
    batcher.send("Second")
    await Task.yield()  // Suspension point
    batcher.send("Third")
    batcher.finish()
}

// Consumer: batches determined by suspension points
for await batch in batcher {
    print(batch)
    // Prints: ["First", "Second"]
    //         ["Third"]
}
```

## SwiftUI Power

Multiple views sending values get batched together automatically:

```swift
let batcher = Batcher<String>()

List {
    Text("First")
        .task { batcher.send("First") }
    Text("Second")
        .task { batcher.send("Second") }
}
.task {
    for await vals in batcher {
        // Process ["First", "Second"] together
        await handleBatch(vals)
    }
}
```

When processing is slow, natural backpressure batches rapid sends:

```swift
for await batch in batcher {
    try await Task.sleep(for: .seconds(1))  // Slow processing
    print(batch)
    // Multiple sends during this sleep get batched in next iteration
}
```

## Lifecycle

- **`send()`** - Add value to buffer
- **`send(contentsOf:)`** - Add multiple values
- **`finish()`** - Terminate sequence (yields remaining buffered values first)

Cancellation of the iteration task immediately terminates the sequence without yielding remaining values.
