using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static void Sink<T>(this DLinqStream<T> input, Action<DLinqBatch<T>> func)
        {
            var comm = input.Communicator;
            if (comm.Rank == input.Sink)
            {
                var stop = false;
                while (!stop)
                {
                    foreach (var rank in input.Operators)
                    {
                        var batch = comm.Receive<DLinqBatch<T>>(rank, 0);
                        func(batch);
                    }
                }
            }
        }
    }
}