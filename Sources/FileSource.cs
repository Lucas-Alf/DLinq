using System.Text;
using DLinq.Stream;
using MPI;

namespace DLinq.Sources
{
    public static class FileSource
    {
        public static DLinqStream<string> ReadFile(Intracommunicator comm, string path, Encoding encoding, int fileBufferSize = 1024, int batchSize = 1)
        {
            if (comm.Rank == 0)
            {
                using (var fileStream = File.OpenRead(path))
                {
                    using (var reader = new StreamReader(fileStream, encoding, true, fileBufferSize))
                    {
                        string? line;
                        var currentRank = 1;
                        var batch = new List<string>();
                        while ((line = reader.ReadLine()) != null)
                        {
                            batch.Add(line);
                            if (batch.Count() == batchSize)
                            {
                                Console.WriteLine($"Rank {comm.Rank} send {batch.Count()} items to rank {currentRank}");
                                comm.Send(batch, currentRank, 0);
                                batch.Clear();

                                currentRank++;
                                if (currentRank == comm.Size)
                                    currentRank = 1;
                            }
                        }
                    }
                }
            }

            return new DLinqStream<string>(comm);
        }
    }
}