using System.Text;
using DLinq.Extensions;
using DLinq.Sources;

namespace DLinq.Exemples
{
    public class StreamWordCount
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var stream = FileSource.ReadFile(comm, "Exemples/StreamWordCount/bible.txt", Encoding.UTF8, batchSize: 1000);
            stream
                .Transformation((input) =>
                {
                    var stopWords = File.ReadAllLines("Exemples/StreamWordCount/stop-words.txt");
                    var result = input
                        .SelectMany(x => x
                            .Replace(".", "")
                            .Replace(",", "")
                            .Trim()
                            .ToLower()
                            .Split(" ")
                        )
                        .Where(x => !stopWords.Contains(x))
                        .GroupBy(x => x)
                        .Select(x => Tuple.Create(x.Key, x.Count()))
                        .ToList();

                    return result;
                })
                .Sink((input) =>
                {
                    var result = input
                        .SelectMany(x => x)
                        .GroupBy(x => x.Item1)
                        .Select(x => Tuple.Create(x.Key, x.Sum(y => y.Item2)))
                        .ToList();

                    Console.WriteLine($"--- Top 10 ---");
                    foreach (var item in result.OrderByDescending(x => x.Item2).Take(10))
                    {
                        Console.WriteLine($"{item.Item1}: {item.Item2}");
                    }
                });
        }
    }
}