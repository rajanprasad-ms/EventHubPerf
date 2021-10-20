using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EHPerf {
    public sealed class MessageListener {

        public delegate void ConsumeMessage(BinaryData message);
        public delegate void StopProcessing();

        /// <summary>
        ///     Constructs an event hub consumer.
        /// </summary>
        public MessageListener(string storageConn, string container, 
            string ehEndpoint, string ehname)  {

            storageConnectionString = storageConn;
            blobContainerName = container;
            checkPointInterval = -1;

            consumerEventHubsConnectionString = ehEndpoint;
            consumerEventHubName = ehname;
            consumerGroup = "$Default";
            consumerWakeUpPeriod = 1000000;
            partitionEventCount = new ConcurrentDictionary<string, int>();
        }
       
        public bool InitConnections() {
            if (null != isPrepared)
                return (bool)isPrepared;

            try {
                storageClient = new BlobContainerClient( storageConnectionString, blobContainerName);
                eventHubConsumer = new EventProcessorClient( storageClient, consumerGroup, 
                    consumerEventHubsConnectionString,
                    consumerEventHubName);
                isPrepared = true;
            }
            catch (Exception ex) {
                Console.Error.WriteLine($"Unable to connect to the event hub or the storage account." +
                    $"{ex.Message}");
                Console.Error.WriteLine($"\t{ex.StackTrace}");

                isPrepared = false;
            }
            return (bool)isPrepared;
        }

        /// <summary>
        ///     Maintains the lifecycle of the eventhub consumer.
        ///     Registers the event handler and the error handler at the start
        ///     and deregisters them after the consumer is stopped.
        /// </summary>
        /// <returns>
        ///     A Task containing the async execution of this method.
        /// </returns>
        public async Task Start()
        {
            if (isPrepared == null || isPrepared == false) {
                await Console.Error.WriteLineAsync("Cannot start Event hub consumer...");
                return;
            }

            try {
                await Console.Out.WriteLineAsync("Starting Event hub consumer...");
                using var cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromMinutes(consumerWakeUpPeriod));
                eventHubConsumer.ProcessEventAsync += ProcessEventHandler;
                eventHubConsumer.ProcessErrorAsync += ProcessErrorHandler;
                await eventHubConsumer.StartProcessingAsync(cancellationSource.Token);
                await Task.Delay(Timeout.Infinite, cancellationSource.Token);

            } catch (TaskCanceledException) {
                await Console.Error.WriteLineAsync($"Event hub consumer cancelled as per timeout provided.");
            } catch (Exception e) {
                await Console.Error.WriteLineAsync("Error in the event hub consumer lifecycle management." + 
                "The consumer might be in an undefined state.");
                await Console.Error.WriteLineAsync($"Exception: { e.Message }");
                await Console.Error.WriteLineAsync($"\tTrace: { e.StackTrace }");
            } finally {
                await Console.Out.WriteLineAsync("Stopping Event hub consumer...");
                await eventHubConsumer.StopProcessingAsync();
                eventHubConsumer.ProcessEventAsync -= ProcessEventHandler;
                eventHubConsumer.ProcessErrorAsync -= ProcessErrorHandler;
                Console.WriteLine(@"Consumer stopped.");
            }
        }
        
        /// <summary>
        ///     Processes an individual event from the event hub.
        ///     Optionally, saves the offset checkpoint for the corresponding partition.
        /// </summary>
        /// <param name="args">
        ///     Args storing the event data and metadata of the event received in event hub.
        /// </param>
        /// <returns>
        ///     A Task object containing the async execution of this method.
        /// </returns>
        private async Task ProcessEventHandler(ProcessEventArgs args) {
            try {
                if (args.CancellationToken.IsCancellationRequested)
                    return;
                
                var partition = args.Partition.PartitionId;
                if (checkPointInterval < 0) return;
                
                var eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
                    partition,1,(_, currentCount) => currentCount + 1);
                
                if (eventsSinceLastCheckpoint < checkPointInterval) return;
                
                await args.UpdateCheckpointAsync();
                partitionEventCount[partition] = 0;

            } catch (Exception e) {
                await Console.Error.WriteLineAsync($"Exception occured while processing event : {e.Message}");
                await Console.Error.WriteLineAsync($"\t{e.StackTrace}");
                await Console.Error.WriteLineAsync("Skipping this event...");
            }
        }
        /// <summary>
        ///     Processes an individual error occured while receiving the event.
        /// </summary>
        /// <param name="args">
        ///     The wrapper of the details of the exception raised by EventHub consumer.
        ///     This exception is not raised by the Event Hub user defined handler methods.
        /// </param>
        /// <returns>
        ///     A Task object containing the <see langword="async"/> execution of this method.
        /// </returns>
        private static Task ProcessErrorHandler(ProcessErrorEventArgs args) {
            try {
                Console.Error.WriteLine("Error in the event hub consumer.");
                Console.Error.WriteLine($"\tOperation: { args.Operation }");
                Console.Error.WriteLine($"\tException: { args.Exception }");
            } catch (Exception e) {
                Console.Error.WriteLine($"Error in handling event hub error. {e.Message}");
            }
            return Task.CompletedTask;
        }


        private readonly string storageConnectionString;
        private readonly string blobContainerName;
        private readonly string consumerEventHubsConnectionString;
        private readonly string consumerEventHubName;
        private readonly string consumerGroup;
        private readonly long consumerWakeUpPeriod;
        private readonly ConcurrentDictionary<string, int> partitionEventCount;
        private readonly int checkPointInterval;
        private BlobContainerClient storageClient;
        private EventProcessorClient eventHubConsumer;

        private bool? isPrepared = null;
    }
}