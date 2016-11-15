using System.IO;

namespace RebusPeekAndReturn.Tests
{
    public class Settings
    {
        public static string AsbConnectionString = File.ReadAllText(@"..\..\..\asbcn.txt");
        public const string From = "Error";
        public const string To = "To";
        public const string DefaultTo = "DefaultTo";
    }
}