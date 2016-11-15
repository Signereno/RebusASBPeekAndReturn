using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Compression;
using Rebus.Config;
using Rebus.Encryption;
using Shouldly;

namespace RebusPeekAndReturn.Tests
{
    [TestFixture]
    public class PeekAndReturn
    {
        private NamespaceManager Manager;
        const string EncryptionKey = "jmbZNVBcpD6gPOv4xs1vbDn0KWOFtLw8LNh0JReQmts=";
        [SetUp]
        public void Setup()
        {
            Manager = NamespaceManager.CreateFromConnectionString(Settings.AsbConnectionString);
            CreateQueueIfNotExists(Settings.From);
            CreateQueueIfNotExists(Settings.To);
            CreateQueueIfNotExists(Settings.DefaultTo);

        }

        private void CreateQueueIfNotExists(string queue)
        {
            if (Manager.QueueExists(queue))
                Manager.DeleteQueue(queue);
            Manager.CreateQueue(queue);
        }
        [Test]
        public async void PeekAndForwardAllMessages()
        {
            var bus = GetBus();

            await bus.Advanced.Routing.Send(Settings.From,
               new SimpleRebusMessage() { Body = "#1 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#2 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            var r = new AzureServicebusPeekAndReturn(Settings.From, Settings.AsbConnectionString, EncryptionKey);
            var peeked = await r.Peek(1000);
            peeked.Count.ShouldBe(2);

            await r.Move((from m in peeked select new MessageToMove() { MessageId = m.MessageId, DestionationQueue = Settings.To }), Settings.DefaultTo);
            var destinationQueueCount = Manager.GetQueue(Settings.To).MessageCount;
            var sourceQueueCount = Manager.GetQueue(Settings.From).MessageCount;

            destinationQueueCount.ShouldBe(2);
            sourceQueueCount.ShouldBe(0);
        }

        [Test]
        public async void PeekAndForwardSomeMessages()
        {
            var bus = GetBus();

            await bus.Advanced.Routing.Send(Settings.From,
               new SimpleRebusMessage() { Body = "#1 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#2 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            var r = new AzureServicebusPeekAndReturn(Settings.From, Settings.AsbConnectionString, EncryptionKey);
            var peeked = await r.Peek(1);
            peeked.Count.ShouldBe(1);

            await r.Move((from m in peeked select new MessageToMove() { MessageId = m.MessageId, DestionationQueue = Settings.To }),Settings.DefaultTo);
            var destinationQueueCount = Manager.GetQueue(Settings.To).MessageCount;
            var sourceQueueCount = Manager.GetQueue(Settings.From).MessageCount;
            var defaultToQueueCount = Manager.GetQueue(Settings.DefaultTo).MessageCount;

            destinationQueueCount.ShouldBe(1);
            sourceQueueCount.ShouldBe(0);
            defaultToQueueCount.ShouldBe(1);
        }



        [Test]
        public async void PeekAndDeleteAllMessages()
        {
            var bus = GetBus();

            await bus.Advanced.Routing.Send(Settings.From,
               new SimpleRebusMessage() { Body = "#1 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#2 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            var r = new AzureServicebusPeekAndReturn(Settings.From, Settings.AsbConnectionString, EncryptionKey);
            var peeked = await r.Peek(1000);
            peeked.Count.ShouldBe(2);

            await r.Move((from m in peeked select new MessageToMove() { Delete = true,MessageId = m.MessageId, DestionationQueue = string.Empty }), string.Empty);
            var destinationQueueCount = Manager.GetQueue(Settings.To).MessageCount;
            var sourceQueueCount = Manager.GetQueue(Settings.From).MessageCount;

            destinationQueueCount.ShouldBe(0);
            sourceQueueCount.ShouldBe(0);
        }

        [Test]
        public async void PeekAndForwardToTwoDifferentQueues()
        {
            var bus = GetBus();

            await bus.Advanced.Routing.Send(Settings.From,
               new SimpleRebusMessage() { Body = "#1 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#2 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            var r = new AzureServicebusPeekAndReturn(Settings.From, Settings.AsbConnectionString, EncryptionKey);
            var peeked = await r.Peek(1000);
            peeked.Count.ShouldBe(2);
            var first = peeked.First();
            var last = peeked.Last();
            await r.Move(new[]
            {
                new MessageToMove() {Delete = false,DestionationQueue = Settings.To,MessageId = first.MessageId,ReturnToSource = false},
                new MessageToMove() {Delete = false,DestionationQueue = Settings.DefaultTo,MessageId = last.MessageId,ReturnToSource = false}
            } , Settings.DefaultTo);

            var destinationQueueCount = Manager.GetQueue(Settings.To).MessageCount;
            var sourceQueueCount = Manager.GetQueue(Settings.From).MessageCount;
            var defaulTotQueueCount = Manager.GetQueue(Settings.DefaultTo).MessageCount;

            destinationQueueCount.ShouldBe(1);
            sourceQueueCount.ShouldBe(0);
            defaulTotQueueCount.ShouldBe(1);
        }


        [Test]
        public async void TestPeekAndReturn()
        {

            var bus = GetBus();

            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#1 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });


            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#2 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });

            await bus.Advanced.Routing.Send(Settings.From,
                new SimpleRebusMessage() { Body = "#3 The body med æ og ø og å", SendtDateTime = DateTime.UtcNow });

            await bus.Advanced.Routing.Send(Settings.From,
               new SimpleRebusMessage() { Body = LargeMessageBody, SendtDateTime = DateTime.UtcNow });


            var r = new AzureServicebusPeekAndReturn(Settings.From, Settings.AsbConnectionString, EncryptionKey);
            var messages = await r.Peek(1000);
            messages.Count.ShouldBe(4);
            Console.WriteLine("================= Peeked messages =========================");
            Console.WriteLine("");
            messages.ForEach(m => Console.WriteLine(JsonConvert.SerializeObject(m)));
            //   await r.Move(messages.Select(m => new MessageToMove() {MessageId = m.MessageId, Delete = true}));

        }

        private static IBus GetBus()
        {

            var adapter = new Rebus.Activation.BuiltinHandlerActivator();
            var bus = Configure.With(adapter)
                .Logging(l => l.ColoredConsole())
                .Options(o =>
                {
                    o.EnableCompression(1);

                    o.EnableEncryption(EncryptionKey);
                })
                .Transport(t => t.UseAzureServiceBusAsOneWayClient(Settings.AsbConnectionString))
                .Start();
            return bus;
        }

        public const string LargeMessageBody =
            @"#4 Lorem Ipsum er rett og slett dummytekst fra og for trykkeindustrien. Lorem Ipsum har vært bransjens standard for dummytekst helt siden 1500-tallet, da en ukjent boktrykker stokket en mengde bokstaver for å lage et prøveeksemplar av en bok. Lorem Ipsum har tålt tidens tann usedvanlig godt, og har i tillegg til å bestå gjennom fem århundrer også tålt spranget over til elektronisk typografi uten vesentlige endringer. Lorem Ipsum ble gjort allment kjent i 1960-årene ved lanseringen av Letraset-ark med avsnitt fra Lorem Ipsum, og senere med sideombrekkingsprogrammet Aldus PageMaker som tok i bruk nettopp Lorem Ipsum for dummytekstasdffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
            fsdaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            asfddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
            fdsssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss.";
    }
}