using System.Linq;
using NUnit.Framework;
using Shouldly;

namespace RebusPeekAndReturn.Tests
{
    [TestFixture]
    public class ProdTest
    {
        [Ignore]
        [Test]
        public async void PeekAndForwardErrorQueue()
        {
            string cn ="";
            var r = new AzureServicebusPeekAndReturn("vegard-z230-queue-error", cn, null);
            var peeked = await r.Peek(1000);
            //Return to source-queue - Fail if not found
            await r.Move(from m in peeked
                select
                    new MessageToMove()
                    {
                        Delete = false,
                        DestionationQueue = string.Empty,
                        ReturnToSource = true,
                        MessageId = m.MessageId
                    }, null);
           
        }
    }
}