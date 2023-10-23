using DLinq.Exemples;

internal class Program
{
    private static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine(@"command: DLinq <mode> <path> <batch-size|threads>
Params descriptions:
mode: -sequential|-parallel|-distributed.
path: file path.
batch-size: number of rows per batch (only used on distributed mode).
threads: number of threads (only used in parallel mode).

WARNING: on distributed mode, the command must be called with mpiexec.
Ex: mpiexec -n <number-of-processes> DLinq -distributed <path> <batch-size> 
");
            Environment.Exit(0);
        }

        var mode = args[0];
        var path = args[1];
        if (String.IsNullOrEmpty(path))
            throw new Exception("Invalid argument: path");

        switch (mode)
        {
            case "-sequential":
                SequentialWordCount.Run(path);
                break;
            case "-parallel":
                {
                    if (args.Length < 3 || String.IsNullOrEmpty(args[2]))
                        throw new Exception("Invalid argument: threads");

                    var threads = Convert.ToInt32(args[2]);
                    ParallelWordCount.Run(path, threads);
                    break;
                }
            case "-distributed":
                {
                    if (args.Length < 3 || String.IsNullOrEmpty(args[2]))
                        throw new Exception("Invalid argument: batch-size");

                    var batchSize = Convert.ToInt32(args[2]);
                    DistributedWordCount.Run(path, batchSize);
                    break;
                }
            default:
                throw new Exception("Invalid argument: mode");
        }
    }
}