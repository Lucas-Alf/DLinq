namespace DLinq.Stores
{
    public class DStreamKeyStore<TKey, TValue> where TKey : notnull
    {
        private static Dictionary<TKey, TValue> Values { get; set; } = new Dictionary<TKey, TValue>();

        public bool ContainsKey(TKey key)
        {
            return Values.ContainsKey(key);
        }

        public void Add(TKey key, TValue value)
        {
            Values.Add(key, value);
        }

        public void Update(TKey key, TValue value)
        {
            Values[key] = value;
        }

        public TValue Get(TKey key)
        {
            return Values[key];
        }

        public IReadOnlyDictionary<TKey, TValue> GetAll()
        {
            return Values.AsReadOnly();
        }

        public void Clear()
        {
            Values.Clear();
        }

        public bool Remove(TKey key)
        {
            return Values.Remove(key);
        }
    }
}