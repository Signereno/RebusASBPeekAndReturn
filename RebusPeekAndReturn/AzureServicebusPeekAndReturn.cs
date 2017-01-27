using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Rebus.AzureServiceBus;
using Rebus.Bus;
using Rebus.Compression;
using Rebus.Encryption;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading.TaskParallelLibrary;

namespace RebusPeekAndReturn
{
    public class AzureServicebusPeekAndReturn
    {
        private readonly string _sourceQueue;
        private readonly string _asbConnectionString;
        private readonly string _encryptionKey;
        readonly IRebusLoggerFactory LoggerFactory = new ConsoleLoggerFactory(true);
        public AzureServicebusPeekAndReturn(string sourceQueue, string asbConnectionString, string encryptionKey = null)
        {
            _sourceQueue = sourceQueue;
            _asbConnectionString = asbConnectionString;
            _encryptionKey = encryptionKey;
        }

        public async Task<int> Handle(IEnumerable<MessageToMove> messagesToHandle, string defaultQueue)
        {
            using (var transport = new AzureServiceBusTransport(_asbConnectionString, _sourceQueue, LoggerFactory, new TplAsyncTaskFactory(LoggerFactory)))
            {
                var messageHandler = new MessageHandler(transport)
                {
                    InputQueue = _sourceQueue,
                    MessagesToHandle = messagesToHandle.ToList(),
                    DefaultOutputQueue = defaultQueue
                };

                await messageHandler.Run();
            }
            return 0;
        }

        public async Task<List<PeekedMessage>> Peek(string messageState = null)
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(_asbConnectionString);

            long count;
            switch (messageState)
            {
                case "Active":// MessageState.Active:
                    count = manager.GetQueue(_sourceQueue).MessageCountDetails.ActiveMessageCount;
                    break;
                case "Scheduled":// MessageState.Scheduled:
                    count = manager.GetQueue(_sourceQueue).MessageCountDetails.ScheduledMessageCount;
                    break;
                default:
                    count = manager.GetQueue(_sourceQueue).MessageCountDetails.ActiveMessageCount +
                    manager.GetQueue(_sourceQueue).MessageCountDetails.ScheduledMessageCount;
                    break;
            }

            return await Peek(Convert.ToInt32(count));
        }

        public async Task<List<PeekedMessage>> Peek(int count, string messageState = null)
        {
            var client = QueueClient.CreateFromConnectionString(_asbConnectionString, _sourceQueue);

            List<BrokeredMessage> brokeredMessages = new List<BrokeredMessage>();
            long i = 0;
            while (i < count)
            {
                IEnumerable<BrokeredMessage> peekBatchBrokeredMessages = await client.PeekBatchAsync(count);
                brokeredMessages.AddRange(peekBatchBrokeredMessages);
                i = i + peekBatchBrokeredMessages.Count();
            }

            var transportMessages = new List<PeekedMessage>();
            foreach (var brokeredMessage in brokeredMessages)
            {
                try
                {
                    if (messageState != null && (MessageState)Enum.Parse(typeof(MessageState), messageState, true) != brokeredMessage.State)
                        continue;

                    var peeked = new PeekedMessage();
                    transportMessages.Add(peeked);
                    var headers = brokeredMessage.Properties
                        .Where(kvp => kvp.Value is string)
                        .ToDictionary(kvp => kvp.Key, kvp => (string)kvp.Value);
                    using (var memoryStream = new MemoryStream())
                    {
                        await brokeredMessage.GetBody<Stream>().CopyToAsync(memoryStream);
                        var transport = new TransportMessage(headers, memoryStream.ToArray());

                        peeked.MessageId = transport.GetMessageId();

                        byte[] bodyBytes = transport.Body;
                        if (headers.ContainsKey(EncryptionHeaders.ContentInitializationVector))
                        {
                            bodyBytes = GetEncryptedBody(bodyBytes, headers[EncryptionHeaders.ContentInitializationVector]);
                        }
                        if (headers.ContainsKey(Headers.ContentEncoding) && headers[Headers.ContentEncoding].Equals("gzip"))
                            bodyBytes = new Zipper().Unzip(bodyBytes);

                        peeked.Body = Encoding.UTF8.GetString(bodyBytes);
                        peeked.Headers = headers;
                    }
                }
                catch
                {
                    // ignored, prevent from crashing 
                }
            }
            return transportMessages;
        }

        private byte[] GetEncryptedBody(byte[] bodyContent, string iv)
        {
            using (var rijndael = new RijndaelManaged())
            {
                rijndael.IV = Convert.FromBase64String(iv);
                rijndael.Key = Convert.FromBase64String(_encryptionKey);

                using (var decryptor = rijndael.CreateDecryptor())
                using (var destination = new MemoryStream())
                using (var cryptoStream = new CryptoStream(destination, decryptor, CryptoStreamMode.Write))
                {
                    cryptoStream.Write(bodyContent, 0, bodyContent.Length);
                    cryptoStream.FlushFinalBlock();
                    return destination.ToArray();
                }
            }
        }
    }
}