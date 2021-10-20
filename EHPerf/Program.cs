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

            CancellationTokenSource source = new CancellationTokenSource();
            if (send)
            {
                MessageSender sender = new MessageSender(ehpoint, ehname, 1, source.Token);
                await sender.DoSend();
            }
            else
            {
                MessageListener listener = new MessageListener(storageconn, blobname, ehpoint, ehname);
                await listener.Start();
            }
        }
    }
}
