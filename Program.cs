using DLinq.Exemples;

internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            if (args.Length < 1)
            {
                Console.WriteLine("DLinq <path> <batch-size>");
                Environment.Exit(0);
            }

            var path = args[0];
            var batchSize = Convert.ToInt32(args[1]);
            WordCount.Run(comm, path, batchSize);
        });
    }
}