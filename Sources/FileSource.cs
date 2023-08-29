using System.Text;
using DLinq.Stream;
using MPI;

namespace DLinq.Sources
{
    public static class FileSource
    {
        public static DLinqStream<string> ReadFile(Intracommunicator comm, string path, Encoding encoding, int batchSize = 1)
        {
            if (comm.Rank == 0)
            {
                using (var fileStream = File.OpenRead(path))
                {
                    using (var reader = new StreamReader(fileStream, encoding))
                    {
                        string? line;
                        var currentRank = 1;
                        var batch = new List<string>();
                        while ((line = reader.ReadLine()) != null)
                        {
                            batch.Add(line);
                            if (batch.Count() == batchSize)
                            {
                                // Console.WriteLine($"Rank {comm.Rank} send {batch.Count()} items to rank {currentRank}");
                                comm.Send(batch, currentRank, 0);
                                batch.Clear();

                                currentRank++;
                                if (currentRank == comm.Size)
                                    currentRank = 1;
                            }
                        }

                        // Send last batch
                        if (batch.Count() != 0)
                        {
                            // Console.WriteLine($"Rank {comm.Rank} send {batch.Count()} items to rank {currentRank}");
                            comm.Send(batch, currentRank, 0);
                            batch.Clear();
                        }

                        // Send End of Signal message
                        for (int rank = 1; rank < comm.Size; rank++)
                            comm.Send(DLinqStream<string>.EOS, rank, 0);
                    }
                }
            }

            return new DLinqStream<string>(comm);
        }
    }
}