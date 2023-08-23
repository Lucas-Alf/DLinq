internal class Program
{
    private static void Main(string[] args)
    {
        MPI.Environment.Run(comm =>
        {
            var input = new List<string> { "um", "dois", "tres", "quatro", "cinco" };
            var result = input
                .DSelect(comm, x => $"{x}-teste");

            if (comm.Rank == 0)
                foreach (var item in result)
                    Console.WriteLine(item);
        });
    }
}