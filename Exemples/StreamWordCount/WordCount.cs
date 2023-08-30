using System.Text;
using DLinq.Extensions;
using DLinq.Sources;

namespace DLinq.Exemples
{
    public class WordCount
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var results = new Dictionary<string, int>();
            var stopWords = File.ReadAllLines("Exemples/StreamWordCount/stop-words.txt");
            var stream = FileSource.ReadFile(comm, "Exemples/StreamWordCount/bible.txt", Encoding.UTF8, batchSize: 1000);
            stream.Transformation((input) =>
                {
                    var result = input.Data
                        .SelectMany(x => x
                            .Replace(".", "")
                            .Replace(",", "")
                            .Replace(":", "")
                            .Replace(";", "")
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
                    foreach (var item in input.Data)
                    {
                        if (results.ContainsKey(item.Item1))
                            results[item.Item1] += item.Item2;
                        else
                            results.Add(item.Item1, item.Item2);
                    }

                    var top5 = results
                        .OrderByDescending(x => x.Value)
                        .Take(10)
                        .Select(x => $"{x.Key}: {x.Value}");

                    var processingTime = DateTime.Now - input.SourceCreatedAt;
                    Console.WriteLine($"({processingTime.Milliseconds}ms to process) -> {String.Join(", ", top5)}");
                });
        }
    }
}