using System;
using System.Collections.Concurrent;
using System.Timers;
using Azure.Messaging.EventHubs.Producer;

namespace EHPerf {
    internal class BatchPool {

        internal BatchPool(EventHubProducerClient client, string partitionKey, int maxBatchSize) {
            this.partitionKey = partitionKey;
            this.maxBatchSize = maxBatchSize;
            this.client = client;
            batchSpawnTimer = new Timer(100);
            batchSpawnTimer.Elapsed += SpawnBatch;
        }

        internal void Start() => batchSpawnTimer.Start();
        internal void Stop() => batchSpawnTimer.Stop();

        internal EventDataBatch Retrieve() {
            Retrieve(out var outBatch);
            return outBatch;
        }

        internal bool Retrieve(out EventDataBatch outBatch) => batchPool.TryDequeue(out outBatch);

        private async void SpawnBatch(object source, ElapsedEventArgs args) {

            if (batchPool.Count >= 10) return;
            await Console.Out.WriteLineAsync("Spawning...");
            batchPool.Enqueue(await client.CreateBatchAsync(new CreateBatchOptions {
                PartitionId = partitionKey,
                MaximumSizeInBytes = maxBatchSize
            }));
        }

        private readonly string partitionKey;
        private readonly int maxBatchSize;
        private readonly EventHubProducerClient client;
        private readonly ConcurrentQueue<EventDataBatch> batchPool = new();
        private readonly Timer batchSpawnTimer;
    }
}