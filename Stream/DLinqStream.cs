using MPI;

namespace DLinq.Stream
{
    public class DLinqStream<T>
    {
        public Intracommunicator Communicator { get; private set; }
        public int Source { get; private set; }
        public int Sink { get; private set; }
        public IEnumerable<int> Operators { get; private set; }

        public DLinqStream(Intracommunicator comm)
        {
            Communicator = comm;
            Source = 0;
            Sink = comm.Size - 1;
            Operators = Enumerable.Range(1, Sink - 1);
        }
    }
}