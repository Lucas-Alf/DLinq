using MPI;

namespace DLinq.Stream
{
    public class DLinqStream<T>
    {
        public Intracommunicator Communicator { get; private set; }

        public DLinqStream(Intracommunicator comm)
        {
            Communicator = comm;
        }
    }
}