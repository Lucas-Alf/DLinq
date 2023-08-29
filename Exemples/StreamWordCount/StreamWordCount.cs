using System.Text;
using DLinq.Extensions;
using DLinq.Sources;

namespace DLinq.Exemples
{
    public class StreamWordCount
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var stream = FileSource.ReadFile(comm, "Exemples/StreamWordCount/bible.txt", Encoding.UTF8, batchSize: 500);
            stream
                .Transformation((input) =>
                {
                    Console.WriteLine($"(Transformation) Rank {comm.Rank} received {input.Count()} records");
                    var result = input
                        .SelectMany(x => x.Split(" "))
                        .GroupBy(x => x)
                        .Select(x => Tuple.Create(x.Key, x.Count()))
                        .ToList();

                    Console.WriteLine($"(Transformation) Rank {comm.Rank} send {input.Count()} records");
                    return result;
                })
                .Sink((input) =>
                {
                    Console.WriteLine($"(Sink) Rank {comm.Rank} received {input.Count()} records");
                    var result = input
                        .SelectMany(x => x)
                        .GroupBy(x => x.Item1)
                        .Select(x => Tuple.Create(x.Key, x.Sum(y => y.Item2)))
                        .ToList();

                    foreach (var item in result.Take(10))
                    {
                        Console.WriteLine($"{item.Item1}: {item.Item2}");
                    }
                });
        }
    }
}