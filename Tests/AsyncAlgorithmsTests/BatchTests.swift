import AsyncAlgorithms
import Foundation
import Testing

struct BatcherTests {
  @Suite("Core functionality")
  struct CoreFunctionality {

    @available(AsyncAlgorithms 1.2, *)
    @Test func initialValuesEmittedFirst() async {
      let batcher = Batcher(initialValues: [1, 2, 3])

      let firstEmission = await batcher.first(where: { @Sendable _ in true })
      #expect(firstEmission == [1, 2, 3])
    }

    @available(AsyncAlgorithms 1.2, *)
    @MainActor @Test func transactionalEmissions() async {
      let batcher = Batcher<Int>()
      let transactionalBatch = Array(0..<10)

      await withDiscardingTaskGroup { g in
        g.addTask {
          await MainActor.run {
            for i in transactionalBatch {
              batcher.send(i)
            }
          }
          batcher.finish()
        }

        let vals = await Array(isolatedSource: batcher)
        #expect(
          vals == [transactionalBatch],
          "Individual values emitted in a single transaction should be batched")
      }
    }

    @available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *)
    @MainActor @Test func sendContentsOfBatches() async {
      let batcher = Batcher<Int>()
      let first = [1, 2, 3]
      let second = [4, 5, 6]

      await withDiscardingTaskGroup { g in
        g.addImmediateTask { @MainActor in
          batcher.send(contentsOf: first)
          await Task.yield()
          batcher.send(contentsOf: second)
          batcher.finish()
        }

        let vals = await Array(isolatedSource: batcher)
        #expect(vals == [first, second])
      }
    }

    @available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *)
    @MainActor @Test func multipleYields() async {
      let batcher = Batcher<Int>()
      let first = Array(0..<10)
      let second = Array(10..<20)
      let batches = [first, second]

      await withDiscardingTaskGroup { g in
        g.addImmediateTask { @MainActor in
          for i in first {
            batcher.send(i)
          }
          await Task.yield()
          for i in second {
            batcher.send(i)
          }
          batcher.finish()
        }

        let elements = await Array(isolatedSource: batcher)
        #expect(elements == batches, "Multiple transactional batches should be emitted")
      }
    }
  }

  @Suite("Finish")
  struct Finish {

    @available(AsyncAlgorithms 1.2, *)
    @Test func finishEmitsAllValuesInBuffer() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addTask {
          batcher.send(1)
          batcher.send(2)
          batcher.finish()
          batcher.send(3)
        }

        let receivedValues: [Int] = await Array(batcher).flatMap(\.self)

        #expect(receivedValues == [1, 2])
      }
    }

    @available(AsyncAlgorithms 1.2, *)
    @Test func finishWithEmptyBufferCompletes() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addTask {
          batcher.finish()
        }

        let vals = await Array(batcher)
        #expect(vals == [])
        // the main test here is that we actually finish
        // if this hangs, the test failed
      }
    }

    @available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *)
    @Test func finishWhileSuspended() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addImmediateTask {
          // Start iteration - will suspend waiting for values
          let vals = await Array(isolatedSource: batcher)
          #expect(
            vals.isEmpty,
            "Should complete without emitting when finished while suspended")
        }

        await Task.yield()
        // Finish while suspended
        batcher.finish()
      }
    }

    @available(AsyncAlgorithms 1.2, *)
    @Test func multipleFinishCalls() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addTask {
          batcher.send(1)
          batcher.finish()
          batcher.finish()
          batcher.finish()
        }

        let emissions = await Array(batcher)

        #expect(emissions == [[1]])
      }
    }

    @available(AsyncAlgorithms 1.2, *)
    @Test func sendAfterFinishIsIgnored() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addTask {
          batcher.send(1)
          batcher.finish()
          batcher.send(2)
          batcher.send(contentsOf: [3, 4])
        }

        let receivedValues = await Array(batcher)

        #expect(receivedValues == [[1]], "Only values sent before finish should be emitted")
      }
    }
  }

  @Suite("Cancel")
  struct Cancel {
    @available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *)
    @MainActor @Test func stopsOnCancelAfterYielding() async {
      let batcher = Batcher<Int>(initialValues: [1])
      await withDiscardingTaskGroup { g in
        // immediate so we ensure we consume the first value in the buffer before cancelling
        g.addImmediateTask { @MainActor in
          let vals = await Array(isolatedSource: batcher)
          #expect(vals == [[1]])
          // if we hang indefinitely. This test failed
        }
        g.cancelAll()
      }
    }

    @available(AsyncAlgorithms 1.2, *)
    @Test func cancelledShouldNotEmitAnyValues() async {
      let batcher = Batcher(initialValues: [1, 2, 3])

      await withDiscardingTaskGroup { g in
        g.cancelAll()
        g.addTask {
          let vals = await Array(batcher)
          #expect(vals == [], "A cancelled sequence should not emit values in its buffer")
        }
      }
    }

    @available(AsyncAlgorithms 1.2, *)
    @Test func cancelationBeforeSend() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addTask {
          let vals = await Array(isolatedSource: batcher)
          #expect(vals == [], "Values emitted after cancel should be ignored")
        }

        // cancel
        g.cancelAll()

        // yield another value
        batcher.send(2)
      }
    }

    @available(macOS 26.0, iOS 26.0, tvOS 26.0, watchOS 26.0, visionOS 26.0, *)
    @Test func cancelWhileSuspended() async {
      let batcher = Batcher<Int>()

      await withDiscardingTaskGroup { g in
        g.addImmediateTask {
          // Start iteration - will suspend waiting for values
          let vals = await Array(isolatedSource: batcher)
          #expect(
            vals.isEmpty,
            "Should complete without emitting when cancelled while suspended")
        }

        await Task.yield()
        // cancel while suspended
        g.cancelAll()
      }
    }
  }

  @Suite("Edge cases")
  struct EdgeCases {
    @available(AsyncAlgorithms 1.2, *)
    @Test func sendEmptyArray() async {
      let batcher = Batcher<Int>()

      await withThrowingTaskGroup { g in
        g.addTask {
          batcher.send(contentsOf: [])
          batcher.send(1)
          batcher.send(contentsOf: [])
          batcher.send(2)
          batcher.finish()
        }
        let receivedValues = await Array(batcher).flatMap(\.self)

        // Empty sends shouldn't affect the actual values received
        #expect([1, 2] == receivedValues)
      }
    }
  }

  @Suite("Concurrency")
  struct Concurrency {
    @available(AsyncAlgorithms 1.2, *)
    @Test func sendATonOfValues() async {
      let batcher = Batcher<Int>()

      let vals = Array(0...1000)
      await withDiscardingTaskGroup { g in
        g.addTask {
          for val in vals {
            batcher.send(val)
          }
          batcher.finish()
        }
        let receivedValues = await Array(batcher).flatMap(\.self)

        // All values should be received in the correct order
        // since the isolation of the iteration and the isolation of the emission are different
        // we can make no transactional guarantees
        #expect(vals == receivedValues)
      }
    }

    // no guarantees can be made about transactionality nor ording but all values gotta be handled
    @available(AsyncAlgorithms 1.2, *)
    @Test func concurrentSends() async {
      let batcher = Batcher<Int>()

      let vals = Array(0...1000)
      await withTaskGroup { g in
        for val in vals {
          g.addTask {
            batcher.send(val)
          }
        }

        await g.waitForAll()
        batcher.finish()

        let receivedValues = await Array(batcher).flatMap(\.self)

        // all vals should be received
        // we can make no guarantees on ordering nor transactionality
        #expect(Set(vals) == Set(receivedValues))
      }
    }
  }
}

@available(AsyncAlgorithms 1.2, *)
extension RangeReplaceableCollection {
  /// Adds isolation to https://github.com/apple/swift-async-algorithms/blob/main/Sources/AsyncAlgorithms/RangeReplaceableCollection.swift
  /// Needed for transactional tests
  fileprivate init<Source: AsyncSequence>(
    isolatedSource source: Source,
    isolation: isolated (any Actor)? = #isolation
  ) async throws(Source.Failure) where Source.Element == Element {
    self.init()
    for try await item in source {
      append(item)
    }
  }
}
