using System.Text;
using DLinq.Stream;
using MPI;

namespace DLinq.Sources
{
    public static class FileSource
    {
        public static DLinqStream<string> ReadFile(Intracommunicator comm, string path, Encoding encoding, int batchSize = 1)
        {
            var stream = new DLinqStream<string>(comm);
            if (comm.Rank == stream.Source)
            {
                using (var fileWatcher = new FileSystemWatcher("."))
                {
                    fileWatcher.Filter = path;
                    fileWatcher.EnableRaisingEvents = true;
                    using (var autoReset = new AutoResetEvent(false))
                    {
                        fileWatcher.Changed += (s, e) => autoReset.Set();
                        using (var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            using (var reader = new StreamReader(fileStream, encoding))
                            {
                                string? line;
                                var sendEOS = true;
                                var batchId = 1L;
                                var firstOperator = stream.Operators.Min();
                                var lastOperator = stream.Operators.Max();
                                var currentRank = firstOperator;
                                var batch = new List<string>();
                                while (true)
                                {
                                    line = reader.ReadLine();
                                    if (line != null)
                                    {
                                        batch.Add(line);
                                        if (batch.Count == batchSize)
                                        {
                                            comm.Send(new DLinqBatch<List<string>>(batchId, batch), currentRank, 0);
                                            batch.Clear();
                                            batchId++;
                                            sendEOS = true;

                                            if (currentRank == lastOperator)
                                                currentRank = firstOperator;
                                            else
                                                currentRank++;
                                        }
                                    }
                                    else
                                    {
                                        // Send last batch
                                        if (batch.Count != 0)
                                        {
                                            comm.Send(new DLinqBatch<List<string>>(batchId, batch), currentRank, 0);
                                            batch.Clear();
                                            batchId++;
                                            sendEOS = true;

                                            if (currentRank == lastOperator)
                                                currentRank = firstOperator;
                                            else
                                                currentRank++;
                                        }

                                        // Send EOS
                                        if (sendEOS)
                                        {
                                            sendEOS = false;
                                            comm.Send(new DLinqBatch<List<string>>(batchId, null, true), currentRank, 0);
                                            batchId++;
                                            if (currentRank == lastOperator)
                                                currentRank = firstOperator;
                                            else
                                                currentRank++;
                                        }

                                        autoReset.WaitOne(1000);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return stream;
        }
    }
}