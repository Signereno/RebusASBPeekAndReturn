using System.Collections.Generic;
using Microsoft.ServiceBus;

namespace RebusPeekAndReturn
{
    public class PeekedMessage
    {
        public string MessageId { get; set; }
        public string Body { get; set; }
        public Dictionary<string,object> Headers { get; set; }
    }
}
    