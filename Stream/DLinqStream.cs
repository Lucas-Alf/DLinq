using MPI;

namespace DLinq.Stream
{
    public class DLinqStream<T>
    {
        public Intracommunicator Communicator { get; private set; }

        public static string EOS { get { return "[MPI-EOS]"; } }

        public DLinqStream(Intracommunicator comm)
        {
            Communicator = comm;
        }
    }
}