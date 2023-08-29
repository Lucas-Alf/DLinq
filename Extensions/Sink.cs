using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static void Sink<T>(this DLinqStream<T> input, Action<T> func)
        {
            var comm = input.Communicator;
            if (comm.Rank == 0)
            {
                while (true)
                {
                    for (int i = 1; i < comm.Size; i++)
                    {
                        var msg = comm.Receive<T>(i, 0);
                        func(msg);
                    }
                }
            }
        }

        // public static void Sink<T>(this DLinqStream<T> input, Action<List<T>> func)
        // {
        //     var comm = input.Communicator;
        //     if (comm.Rank == 0)
        //     {
        //         var temp = new List<T>();
        //         for (int i = 1; i < comm.Size; i++)
        //         {
        //             var msg = comm.Receive<T>(i, 0);
        //             temp.Add(msg);
        //         }

        //         func(temp);
        //     }
        // }
    }
}