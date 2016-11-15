# RebusASBPeekAndReturn
Simple utility for peeking and forwarding/returning REBUS messages in ASB


```c#
string cn ="";
var r = new AzureServicebusPeekAndReturn("vegard-z230-queue-error", cn, null);
//Peek first 1000 messages
var peeked = await r.Peek(1000);
//Return to source-queue - Fail if not found
await r.Move(from m in peeked
    select
        new MessageToMove()
        {
            Delete = false, //Do not delete
            DestionationQueue = string.Empty, //No destinationqueue, rely on sourceque-header
            ReturnToSource = true, //Use sourcequeueheader as source
            MessageId = m.MessageId //Message to forward
        }, null); //null==no defaultqueue for non-peeked messages

```