using System.Collections.Generic;
using Microsoft.ServiceBus.Messaging;

namespace RebusPeekAndReturn
{
    public class PeekedMessage
    {
        public string MessageId { get; set; }
        public string Body { get; set; }
        public Dictionary<string,string> Headers { get; set; }
    }

    public class PeekedMessagePage
    {
        public IEnumerable<PeekedMessage> Messages { get; set; }
        public long NextSequenceNumber { get; set; }
    }
}
    