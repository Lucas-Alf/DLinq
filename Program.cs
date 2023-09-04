using DLinq.Exemples;

internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            // WordCount.Run(comm);
            ObjectDetection.Run(comm);
        });
    }
}