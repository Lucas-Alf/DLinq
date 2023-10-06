using DLinq.Exemples;

internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            if (args.Length < 1)
            {
                Console.WriteLine("dlinq <path> <batch-size>");
                Environment.Exit(0);
            }

            if (!MPI.Environment.IsTimeGlobal)
                throw new Exception("Time is not synchronized between processes");

            var path = args[0];
            var batchSize = Convert.ToInt32(args[1]);
            WordCount.Run(comm, path, batchSize);
        });
    }
}