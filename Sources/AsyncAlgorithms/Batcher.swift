import OSLog
import SwiftUI
import Synchronization

@available(AsyncAlgorithms 1.2, *)
private let logger = Logger(subsystem: "swift-async-algorithms", category: "Batcher")

/// An `AsyncSequence` that batches values and yields them transactionally on demand.
///
/// Values sent to a `Batcher` accumulate until downstream iteration requests them.
/// Each iteration yields all buffered values as a single array, using suspension points
/// as transactional boundaries—similar to how `Observation` batches mutations between
/// `willSet` and the next suspension point.
///
/// - No backpressure: `send` never blocks
/// - Single consumer: supports one active iteration
@available(AsyncAlgorithms 1.2, *)
public struct Batcher<Value: Sendable>: AsyncSequence, Sendable {

  private let state: CriticalState<State>

  /// Creates a batcher with optional initial values in the buffer.
  public init(initialValues: [Value] = []) {
    self.state = CriticalState(initialValue: .bufferringValues(initialValues))
  }

  /// Adds a value to the buffer. Never blocks.
  public func send(_ value: Value) {
    send(contentsOf: [value])
  }

  /// Adds values to the buffer. Never blocks.
  public func send(contentsOf values: some Collection<Value>) {
    guard !values.isEmpty else {
      logger.debug("Received empty values. Ignoring")
      return
    }

    logger.debug("Received values \(String(describing: values))")
    state.withLock { $0.receive(values) }
  }

  /// Terminates the sequence. Buffered values are yielded before completion.
  public func finish() {
    state.withLock { $0.transitionToFinished() }
  }

  /// I think this can be transitioned into three states
  /// 1. We have values to return immediately. Waiting for downstream demmand. This is when values accumulate
  /// 2. We dont have anything to return immediately which means we have a continuation. When we get values, the continuation gets resumed and we get the buffer going
  /// 3. ? we are terminated
  enum State {
    case bufferringValues([Value])
    case waitingForMoreValues(CheckedContinuation<Void, Never>)
    /// remaining values is for when we transfer from buffering to terminated but want to yield what we have up to this point
    case terminated(remainingValues: [Value])

    /// Consumes the buffer. Invalid to call when we are waiting for values
    mutating func consumeBuffer() -> [Value]? {
      switch self {
      case .bufferringValues(let values):
        if Task.isCancelled {
          logger.debug(
            "ConsumeBuffer: Task is cancelled. Clearing buffer and setting state to terminated"
          )
          self = .terminated(remainingValues: [])
          return nil
        } else {
          logger.debug("ConsumeBuffer: Consuming buffer")
          self = .bufferringValues([])
          return values
        }
      case .terminated(let remainingValues):
        // if we are terminated but we still have values in our buffer, yield and consume them
        // the next iteration will finish the sequence
        if !remainingValues.isEmpty {
          logger.debug("ConsumeBuffer: Yielding remaining values")
          self = .terminated(remainingValues: [])
          return remainingValues
        } else {
          logger.debug("ConsumeBuffer: Sequence terminated")
          return nil
        }
      case .waitingForMoreValues:
        // the iterator is either being consumed across actor contexts or we screwed something up
        fatalError(
          "Cannot consume buffer while waiting for values. Invalid state. Iterated concurrently or we screwed up"
        )
      }
    }

    mutating func receive(_ values: some Collection<Value>) {
      // 1. We don't have any downstream demand, values should go into the buffer
      // 2. We have downstream demand, add new values to buffer and resume the continuation
      // 3. We are terminated, this is a no-op
      switch self {
      case .bufferringValues(var bufferedValues):
        logger.debug("No downstream demand, adding to buffer")
        bufferedValues.append(contentsOf: values)
        self = .bufferringValues(bufferedValues)
      case .waitingForMoreValues(let continuation):
        logger.debug(
          "Have downstream demand, resuming continuation and transitioning to buffering values"
        )
        continuation.resume()
        self = .bufferringValues(Array(values))
      case .terminated:
        logger.debug("Values received but we're terminated, ignoring")
      }
    }

    mutating func transitionToFinished() {
      // if we are buffering, stop accepting new values, yield whatever we got the next chance we get.
      // if we are waiting for more values, resume the continuation, transition to terminated without emitting anymore vals
      switch self {
      case .bufferringValues(let bufferedValues):
        logger.debug(
          "Received finish, transitioning from buffering to terminated. Moving vals \(bufferedValues) over"
        )
        self = .terminated(remainingValues: bufferedValues)
      case .waitingForMoreValues(let continuation):
        logger.debug("Transitioning from waiting for more to terminated")
        self = .terminated(remainingValues: [])
        continuation.resume()
      case .terminated:
        logger.debug("Received finish but were already terminated. Ignoring")
      }
    }

    mutating func handleSuspension(continuation: CheckedContinuation<Void, Never>) {
      guard !Task.isCancelled else {
        // we were cancelled before we had the chance to store the continuation. Resume immediately
        logger.debug(
          "Iterator: Task was cancelled. Return immediately and set state")
        continuation.resume()
        self = .terminated(remainingValues: [])
        return
      }
      switch self {
      case .bufferringValues(let bufferedValues):
        if bufferedValues.isEmpty {
          // we didnt receive any new values between the consumption of values and now. actually suspend
          logger.debug(
            "Iterator: Setting continuation and awaiting new values")
          self = .waitingForMoreValues(continuation)
        } else {
          // we received new values while waiting to suspend.
          // resume the continuation immediately and don't touch the state
          logger.debug(
            "Iterator: Received values \(bufferedValues) while waiting to suspend. Continue immediately"
          )
          continuation.resume()
        }
      case .waitingForMoreValues:
        fatalError("Iterated concurrently, not allowed")
      case .terminated:
        // received termination while waiting to suspend
        // continue immediately
        logger.debug(
          "Iterator: Received termination while waiting to suspend. Continue immediately"
        )
        continuation.resume()
      }
    }

    mutating func handleCancellation() {
      if case .waitingForMoreValues(let continuation) = self {
        logger.debug(
          "Received cancellation while waiting for more values. Resuming and transitioning to terminated"
        )
        continuation.resume()
        self = .terminated(remainingValues: [])
      }
    }
  }
}

@available(AsyncAlgorithms 1.2, *)
extension Batcher {
  public struct Iterator: AsyncIteratorProtocol {
    fileprivate let state: CriticalState<State>

    fileprivate init(state: CriticalState<State>) {
      self.state = state
    }

    public func next(isolation iterationIsolation: isolated (any Actor)?) async -> [Value]? {
      let values = state.withLock { $0.consumeBuffer() }

      guard let values else {
        return nil
      }

      // if we have no values in our buffer, suspend until we do
      // its the responsibility of the `send` methods to resume us
      if values.isEmpty {
        logger.debug("Iterator: No values in buffer. Attempting to suspend")

        // note that body is not called synchronously
        // event though the buffer was empty here doesn't mean its empty in the body of withCheckedContinuation
        // must reevaluate state
        await withTaskCancellationHandler {
          await withCheckedContinuation { continuation in
            state.withLock { $0.handleSuspension(continuation: continuation) }
          }
        } onCancel: { [state] in
          state.withLock { $0.handleCancellation() }
        }

        // is there a time between the synchronous continuation.resume and us getting to this point?
        // Even if there is, I think its okay since we acquire the lock again
        // the only thing it can't transition to is waitingForValues which is impossible unless this iterator is iterated more than once
        logger.log("Iterator: Suspension resumed")

        // grab the values after suspension resumes
        // This is what allows for transactionality
        return state.withLock { $0.consumeBuffer() }
      } else {
        logger.debug("Iterator: Values in buffer. Returning immediately")
        return values
      }
    }

  }

  public func makeAsyncIterator() -> Iterator {
    Iterator(state: state)
  }
}

// should be replaced with `ManagedCriticalState`
@available(AsyncAlgorithms 1.2, *)
private final class CriticalState<T: Sendable>: Sendable {
    let value: Mutex<T>

    init(initialValue: T) {
        self.value = Mutex(initialValue)
    }

    borrowing func withLock<Result, E>(_ body: (inout sending T) throws(E) -> sending Result)
        throws(E) -> sending Result where E: Error, Result: ~Copyable
    {
        try value.withLock(body)
    }
}

/// this is one one of the super powers of batcher
/// even though the values are being sent from different tasks,
/// the transactionality of batcher allows them to get processed together
@available(AsyncAlgorithms 1.2, *)
#Preview("Transactionality") {
  let batcher = Batcher<String>()
  List {
    Text("First")
      .task {
        batcher.send("First")
      }
    Text("Second")
      .task {
        batcher.send("Second")
      }
    Text("Third")
      .task {
        // since we have a suspension
        // this will be in its batch
        // Note: Task.yield worked inconsistently so I went with sleep
        try? await Task.sleep(for: .milliseconds(10))
        batcher.send("Third")
      }
  }
  .task {
    for await vals in batcher {
      print(vals)
      // prints:
      //     ["First", "Second"]
      //     ["Third"]
    }
  }
}

/// this is the other super power of batcher. This simulates batches taking a decent amount of time to proccess
/// In this example, "First" is emitted immediately, "Second" after 100 ms and "Third" after 200 ms
/// Without any processing backpressure, this would be emitted in three separate batches
/// But since processing takes 1s,
/// The proccess goes like this
/// 1. First gets sent immediately and begins processing. This holds up the loop for 1s
/// 2. after 100 ms, "Second" gets sent, because we are in the middle of processing "First", it gets added to the next batch
/// 3. after 200 ms, "Third" gets sent. because we are still processing "First". "Third" gets batched with second
/// 4. Once "First" finishes, "Second" and "Third" are then processed together!
@available(AsyncAlgorithms 1.2, *)
#Preview("Batching") {
  @Previewable @State var batcher = Batcher<String>()
  List {
    Text("First")
      .task {
        batcher.send("First")
      }
    Text("Second")
      .task {
        try? await Task.sleep(for: .milliseconds(100))
        batcher.send("Second")
      }
    Text("Third")
      .task {
        try? await Task.sleep(for: .milliseconds(200))
        batcher.send("Third")
      }
  }
  .task {
    for await vals in batcher {
      // similate that a batch takes a second to proccess
      try? await Task.sleep(for: .seconds(1))
      print(vals)
      // prints:
      //     ["First"]
      //     ["Second", "Third"]
    }
  }
}
