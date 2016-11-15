using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
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

        public async Task<int> Move(IEnumerable<MessageToMove> messagesToMove,string defaultQueue)
        {
            using (var transport = new AzureServiceBusTransport(_asbConnectionString, _sourceQueue, LoggerFactory, new TplAsyncTaskFactory(LoggerFactory)))
            {
                var returnToSourceQueue = new ReturnToSourceQueue(transport)
                {
                    InputQueue = _sourceQueue,
                    DefaultOutputQueue = defaultQueue,
                    ToMove = messagesToMove
                };

                await returnToSourceQueue.Run();
            }
            return 0;
        }

        public async Task<List<PeekedMessage>> Peek(int count)
        {
   
            var client = QueueClient.CreateFromConnectionString(_asbConnectionString, _sourceQueue);

            var messages = await client.PeekBatchAsync(count);
            var transportMessages = new List<PeekedMessage>();
            foreach (var brokeredMessage in messages)
            {
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