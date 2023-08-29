using MPI;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static IEnumerable<TResult> DRun<T, TResult>(this IEnumerable<T> input, Intracommunicator comm, Func<IEnumerable<T>, TResult> func)
        {
            var totalRecords = input.Count();
            var batchSize = GetBatchSize(comm.Size, totalRecords);
            var result = new TResult[] { };
            for (int rank = 0; rank < comm.Size; rank++)
            {
                if (rank == comm.Rank)
                {
                    var skip = rank * batchSize.Item1;
                    var take = rank != comm.Size - 1
                        ? batchSize.Item1
                        : batchSize.Item2;

                    Console.WriteLine($"(DRun) Rank {comm.Rank} take {take} records");
                    var temp = func(input.Skip(skip).Take(take));

                    comm.Barrier();
                    var gather = comm.Gather(temp, 0);
                    if (comm.Rank == 0)
                        result = gather;

                    // comm.Broadcast(ref result, 0);
                }
            }

            return result;
        }

        // public static IEnumerable<TResult> Transformation<T, TResult>(this IEnumerable<T> input, Intracommunicator comm, Func<IEnumerable<T>, TResult> func)
        // {
        //     if (comm.Rank != 0)
        //     {
        //         var msg = comm.Receive<IEnumerable<T>>(0, 0);
        //         var result = func(msg);
        //         comm.Send(result, 0, 0);
        //     }

        //     return new TResult[] { };
        // }
    }
}