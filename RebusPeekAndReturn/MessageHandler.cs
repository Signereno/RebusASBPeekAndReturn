using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Messages;
using Rebus.Transport;

namespace RebusPeekAndReturn
{
    public class MessageHandler
    {
        private readonly HashSet<string> _initializedQueues = new HashSet<string>();
        private readonly ITransport _transport;

        public MessageHandler(ITransport transport)
        {
            _transport = transport;
        }

        public string DefaultOutputQueue { get; set; }
        public string InputQueue { get; set; }
        public IList<MessageToMove> MessagesToHandle { get; set; }

        public async Task HandleMessage(TransportMessage transportMessage, ITransactionContext transactionContext)
        {
            var instruct = MessagesToHandle.FirstOrDefault(m => m.MessageId.Equals(transportMessage.GetMessageId()));

            //Move message to preferred channel
            if (instruct != null)
            {
                if (!string.IsNullOrEmpty(instruct.DestionationQueue))
                {
                    await MoveMessage(transportMessage, transactionContext, instruct.DestionationQueue);
                }
                else if (instruct.Delete)
                {
                    //noop
                }
                //Move message to sourcequeue
                else if (instruct.ReturnToSource)
                {
                    if (transportMessage.Headers.ContainsKey(Headers.SourceQueue))
                    {
                        var sourceQueue = transportMessage.Headers[Headers.SourceQueue];

                        await MoveMessage(transportMessage, transactionContext, sourceQueue);
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"Cannot move message with id {instruct.MessageId} to source queue beacause header does not exist");
                    }
                }
                MessagesToHandle.Remove(instruct);
            }
            else
            {
                if (string.IsNullOrEmpty(DefaultOutputQueue))
                    throw new ArgumentException("Default queue cannot be null when trying to forward message to default queue");

                await MoveMessage(transportMessage, transactionContext, DefaultOutputQueue);
            }
        }

        private async Task MoveMessage(TransportMessage transportMessage, ITransactionContext transactionContext,
            string destinationQueue)
        {
            if (!_initializedQueues.Contains(destinationQueue))
            {
                _transport.CreateQueue(destinationQueue);
                _initializedQueues.Add(destinationQueue);
            }

            await _transport.Send(destinationQueue, transportMessage, transactionContext);
        }

        public async Task Run()
        {
            while (true)
            {
                using (var transactionContext = new DefaultTransactionContext())
                {
                    var transportMessage = await _transport.Receive(transactionContext, new CancellationTokenSource().Token);

                    if (!MessagesToHandle.Any() || transportMessage == null) break;

                    await HandleMessage(transportMessage, transactionContext);
                    
                    await transactionContext.Complete();
                }
            }
        }
    }
}
