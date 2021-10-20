using System;
using System.Threading;
using System.Threading.Tasks;

namespace EHPerf {
    class Program {
        static async Task Main(string[] args) {
            Console.WriteLine("Hello World!");

            var send = true;
            string ehpoint = "";
            string ehname = "";
            string storageconn = "";
            string blobname = "";
            var source = new CancellationTokenSource(1000000);
            if (send) {
                var sender = new MessageSender(ehpoint, ehname, 1, source.Token);
                await sender.DoSend();
                

            }
            else
            {
                var count = 64;
                var listeners = new MessageListener[count];
                for (int i = 0; i < listeners.Length; i++) {
                    listeners[i] = new MessageListener(storageconn, blobname, ehpoint, ehname);
                    listeners[i].InitConnections();
                }

                var tasks = new Task[count];
                for (int i = 0; i < count; i++)
                    tasks[i] = listeners[i].Start();

                Task.WhenAll(tasks).Wait();

            }
        }
    }
}
