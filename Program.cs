using DLinq.Exemples;

internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            // PrimeNumbers.Run(comm);
            StreamWordCount.Run(comm);
        });
    }
}