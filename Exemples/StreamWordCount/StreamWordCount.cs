using System.Text;
using DLinq.Extensions;
using DLinq.Sources;

namespace DLinq.Exemples
{
    public class StreamWordCount
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var stream = FileSource.ReadFile(comm, "Exemples/StreamWordCount/test.txt", Encoding.UTF8, batchSize: 5);
            stream
                .Transformation((input) =>
                {
                    Console.WriteLine($"Rank {comm.Rank} received {input.Count()} items");
                    return input
                        .SelectMany(x => x.Split(" "))
                        .GroupBy(x => x)
                        .Select(x => Tuple.Create(x.Key, x.Count()))
                        .ToList();
                })
                .Sink((input) =>
                {
                    Console.WriteLine($"Rank {comm.Rank} received {input.Count()} items");
                    var result = input
                        .GroupBy(x => x)
                        .Select(x => Tuple.Create(x.Key, x.Sum(y => y.Item2)))
                        .ToList();

                    foreach (var item in result)
                    {
                        Console.WriteLine($"{item.Item1}: {item.Item2}");
                    }
                });
        }
    }
}