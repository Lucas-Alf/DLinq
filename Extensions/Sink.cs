using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static void Sink<T>(this DLinqStream<T> input, Action<List<T>> func)
        {
            var comm = input.Communicator;
            if (comm.Rank == input.Sink)
            {
                var stop = false;
                var data = new List<T>();
                while (!stop)
                {
                    foreach (var rank in input.Operators)
                    {
                        var record = comm.Receive<DLinqRecord<T>>(rank, 0);
                        if (record.EOS)
                        {
                            stop = true;
                            break;
                        }

                        data.Add(record.Data);
                    }
                }

                func(data);
            }
        }
    }
}