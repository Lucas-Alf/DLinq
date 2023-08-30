using System.Text;
using DLinq.Stream;
using MPI;

namespace DLinq.Sources
{
    public static class FileSource
    {
        public static DLinqStream<string> ReadFile(Intracommunicator comm, string path, Encoding encoding, int batchSize = 5000)
        {
            var stream = new DLinqStream<string>(comm);
            if (comm.Rank == stream.Source)
            {
                using (var fileStream = File.OpenRead(path))
                {
                    using (var reader = new StreamReader(fileStream, encoding))
                    {
                        string? line;
                        var firstOperator = stream.Operators.Min();
                        var lastOperator = stream.Operators.Max();
                        var currentRank = firstOperator;
                        var batch = new List<string>();
                        while ((line = reader.ReadLine()) != null)
                        {
                            batch.Add(line);
                            if (batch.Count() == batchSize)
                            {
                                comm.Send(new DLinqRecord<List<string>>(batch), currentRank, 0);
                                batch.Clear();

                                if (currentRank == lastOperator)
                                    currentRank = firstOperator;
                                else
                                    currentRank++;
                            }
                        }

                        // Send last batch
                        if (batch.Count() != 0)
                        {
                            comm.Send(new DLinqRecord<List<string>>(batch), currentRank, 0);
                            batch.Clear();
                        }

                        // Send End of Signal message
                        var eosRecord = new DLinqRecord<List<string>>(null, true);
                        foreach (var rank in stream.Operators)
                            comm.Send(eosRecord, rank, 0);
                    }
                }
            }

            return stream;
        }
    }
}