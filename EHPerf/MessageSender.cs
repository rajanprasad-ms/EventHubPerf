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
            //for (int i = 0; i < batchPools.Capacity; i++)
            //{
            //    var batchPool = new BatchPool(mEventHubClient, i.ToString(), 10000);
            //    this.batchPools.Add(batchPool);
            //    batchPool.Start();
            //}
        }

        

        public async Task DoSend()
        {

            EventData data = new EventData(new BinaryData(new byte[10000]));

            Parallel.For(0, 1000, (i) =>{
                    while (!cancellation.IsCancellationRequested) {
                        //                    EventDataBatch batch = batchPools[i].Retrieve();
                        if(i > 0) i = 0;
                        if(i >= 1000) i = 999;
                        var batchTask = mEventHubClient.CreateBatchAsync();
                        batchTask.AsTask().Wait();
                        var batch = batchTask.Result;
                        batch.TryAdd(data);
                        var success = false;

                        while (!success) {
                            try {
                                mEventHubClient.SendAsync(batch).Wait();
                                success = true;
                            } catch (Exception ex) {
                                Console.Error.WriteLine($@"Error while sending to event hub. {ex.Message}");
                                Task.Delay(maxTimerInterval).Wait();
                            }
                        }
                    }

                    Console.Out.WriteLine($"Cancelled... {i}");
                });

            
        }

        private readonly EventHubProducerClient mEventHubClient;
        private readonly int maxTimerInterval;
        private readonly CancellationToken cancellation;
        //private readonly List<BatchPool> batchPools = new (1000);

    }

}
