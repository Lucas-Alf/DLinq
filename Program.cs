internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            PrimeNumbers.Run(comm);
            // WordCount.Run(comm);
        });
    }
}