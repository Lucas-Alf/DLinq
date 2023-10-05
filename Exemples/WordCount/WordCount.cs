using System.Text;
using DLinq.Extensions;
using DLinq.Sources;
using DLinq.Stores;

namespace DLinq.Exemples
{
    public class WordCount
    {
        public static void Run(MPI.Intracommunicator comm, string path, int batchSize)
        {
            var stream = FileSource.ReadFile(comm, path, Encoding.UTF8, batchSize);
            stream.Transformation((input) =>
                {
                    return input.Data!
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
                })
                .Sink((input) =>
                {
                    var store = new DStreamKeyStore<string, int>();
                    if (!input.EOS)
                    {
                        foreach (var data in input.Data!)
                        {
                            if (store.ContainsKey(data.Item1))
                            {
                                var currentValue = store.Get(data.Item1);
                                store.Update(data.Item1, currentValue + data.Item2);
                            }
                            else
                            {
                                store.Add(data.Item1, data.Item2);
                            }
                        }
                    }
                    else
                    {
                        var topResults = store.GetAll()
                            .OrderByDescending(x => x.Value)
                            .Take(10)
                            .Select(x => $"{x.Key}: {x.Value}");

                        Console.WriteLine(String.Join(", ", topResults));
                    }
                });
        }
    }
}