using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static void Sink<T>(this DLinqStream<T> input, Action<List<T>> func)
        {
            var comm = input.Communicator;
            if (comm.Rank == 0)
            {
                var stop = false;
                var data = new List<T>();
                while (!stop)
                {
                    for (int i = 1; i < comm.Size; i++)
                    {
                        var msg = comm.Receive<object>(i, 0);
                        if (msg is string && msg.ToString() == DLinqStream<T>.EOS)
                        {
                            stop = true;
                            break;
                        }

                        data.Add((T)msg);
                        Console.WriteLine($"Sink size: {data.Count()}");
                    }
                }

                func(data);
            }
        }
    }
}