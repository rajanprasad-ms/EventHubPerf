using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Timer = System.Timers.Timer;

namespace EHPerf {

    public sealed class MessageSender {
        public MessageSender(string ehEndpoint, string ehname, int maxTimerIntervalInMs,
            CancellationToken token) {
            mEventHubClient = new EventHubProducerClient(ehEndpoint, ehname);
            this.cancellation = token;
	        maxTimerInterval = maxTimerIntervalInMs;
            for (int i = 0; i < batchPools.Capacity; i++)
            {
                var batchPool = new BatchPool(mEventHubClient, i.ToString(), 10000);
                this.batchPools.Add(batchPool);
                batchPool.Start();
            }
        }

        

        public void DoSend()
        {

            EventData data = new EventData(new BinaryData(new byte[10000]));
            Parallel.For(0, batchPools.Count, async i =>
            {
                while (!cancellation.IsCancellationRequested)
                {
                    EventDataBatch batch = batchPools[i].Retrieve();
                    if (null == batch)
                    {
                        await Console.Error.WriteLineAsync($@"Error dequeing from pool.");
                        await Task.Delay(maxTimerInterval);
                        continue;
                    }
                    batch.TryAdd(data);
                    var success = false;

                    while (!success) {
                        try {
                            await mEventHubClient.SendAsync(batch);
                            success = true;
                        } catch (Exception ex) {
                            await Console.Error.WriteLineAsync($@"Error while sending to event hub. {ex.Message}");
                            await Task.Delay(maxTimerInterval);
                        }
                    }
                }
            });
            
            
        }

        private readonly EventHubProducerClient mEventHubClient;
        private readonly int maxTimerInterval;
        private readonly CancellationToken cancellation;
        private readonly List<BatchPool> batchPools = new (1000);

    }

}
