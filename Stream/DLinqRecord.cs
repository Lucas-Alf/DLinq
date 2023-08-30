namespace DLinq.Stream
{
    [Serializable]
    public class DLinqRecord<T>
    {
        /// <summary>
        /// Record content
        /// </summary>
        public T Data { get; private set; }

        /// <summary>
        /// End of Signal
        /// </summary>
        public bool EOS { get; private set; }

        public DLinqRecord(T data, bool eos = false)
        {
            Data = data;
            EOS = eos;
        }
    }
}