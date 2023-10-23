using System.Diagnostics;

namespace DLinq.Exemples
{
    public class SequentialWordCount
    {
        public static void Run(string path)
        {
            Console.WriteLine("Mode: sequential");
            var stopWatch = Stopwatch.StartNew();
            var result = File.ReadAllLines(path)
                .Where(line => !string.IsNullOrEmpty(line))
                .SelectMany(line => line
                    .Replace(".", "")
                    .Replace(",", "")
                    .Replace(":", "")
                    .Replace(";", "")
                    .Trim()
                    .ToLower()
                    .Split(" ")
                )
                .GroupBy(word => word)
                .Select(word => Tuple.Create(word.Key, word.Count()))
                .ToList();
            stopWatch.Stop();

            var topResults = result
                .OrderByDescending(x => x.Item2)
                .Take(10)
                .Select(x => $"{x.Item1}: {x.Item2}");

            Console.WriteLine(String.Join(", ", topResults));
            Console.WriteLine($"Elapsed time: {stopWatch.ElapsedMilliseconds}ms");
        }
    }
}