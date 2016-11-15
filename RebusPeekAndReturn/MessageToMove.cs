namespace RebusPeekAndReturn
{
    public class MessageToMove
    {
        public string MessageId { get; set; }
        public string NewMessageBody { get; set; }
        public bool Delete { get; set; }
        public string DestionationQueue { get; set; }
        public bool ReturnToSource { get; set; }
    }
}