public class WordCount
{
    public static void Run(MPI.Intracommunicator comm)
    {
        var input = File.ReadAllLines("Exemples/WordCount/bible.txt");
        var result = input
            .DRun(comm, (x) =>
            {
                var words = x
                    // .AsParallel()
                    .SelectMany(x => x.ToLower().Split(" "))
                    .GroupBy(x => x)
                    .Select(x => Tuple.Create(x.Key, x.Count()))
                    .ToList();

                return words;
            });

        if (comm.Rank == 0)
        {
            var aggregate = result
                .SelectMany(x => x)
                .GroupBy(x => x.Item1)
                .Select(x => Tuple.Create(x.Key, x.Sum(y => y.Item2)))
                .ToList();

            Console.WriteLine("--- Top 10 words ---");
            foreach (var item in aggregate.OrderByDescending(x => x.Item2).Take(10))
            {
                Console.WriteLine($"{item.Item1}: {item.Item2}");
            }

            Console.WriteLine("--- Final Result ---");
            Console.WriteLine($"{aggregate.Sum(x => x.Item2)} words!");
        }
    }
}