using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Newtonsoft.Json;
using ServiceStack.Redis.Generic;
using StackExchange.Redis;

namespace RedisCache
{
    class Program
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            string cacheConnection = "prod-navi-sf.redis.cache.windows.net:6380,password=YX+fJAqjlNJGA1CF8pZoqI0mreBqVJcedRR4LbG3PHg=,ssl=True,abortConnect=False";
            return ConnectionMultiplexer.Connect(cacheConnection);
        });

        public static ConnectionMultiplexer Connection => lazyConnection.Value;

        static void Main(string[] args)
        {
            // Connection refers to a property that returns a ConnectionMultiplexer
            // as shown in the previous example.
            IDatabase cache = Connection.GetDatabase();

            // Perform cache operations using the cache object...

            // Simple PING command
            string cacheCommand = "PING";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            Console.WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());

            Console.WriteLine("Saving random data in cache");
            SaveBigData();

            Console.WriteLine("Reading data from cache");
            //ReadData();

            Console.ReadLine();

            lazyConnection.Value.Dispose();
        }

        public static void SaveBigData()
        {
            var devicesCount = 15000;
            var rnd = new Random();
            var cache = Connection.GetDatabase();

            List<EncCell> encCells = new List<EncCell>();

            for (int i = 1; i < devicesCount; i++)
            {
                encCells.Add(new EncCell
                {
                    Id = i,
                    CellName = $"ABCDEFGH{i}",
                    AddedDate = DateTime.UtcNow,
                    BaseEdition = 1,
                    BaseIssueDate = DateTime.UtcNow,
                    BoundaryELon = 15,
                    BoundaryNLat = 10,
                    BoundarySLat = 12,
                    BoundaryWLon = 14,
                    CancelDateUtc = DateTime.UtcNow,
                    Description = $"Demo Description here to be added{i}",
                    DownloadedBaseEdition = 12,
                    DownloadedIhoBaseEdition = 10,
                    DownloadedIhoUpdateNumber = 5,
                    DownloadedIhoUtcDate = DateTime.UtcNow,
                    DownloadedUpdateNumber = 8,
                    DownloadedUtcDate = DateTime.UtcNow,
                    Geography = $"Demo Geography here to be added{i}",
                    Geometry = $"Demo Geometry here to be added{i}",
                    IsForSale = true,
                    GeometryData = $"Demo GeometryData here to be added{i}",
                    IssuerId = 1,
                    Issuer = new Issuer
                    {
                        Id = 1,
                        Active = true,
                        Name = "Primar",
                        ShortName = "PR"
                    },
                    LatestUpdateNumber = 10,
                    PolygonPoints = $"Demo PolygonPoints here to be added{i}",
                    RecordLastUpdated = DateTime.UtcNow,
                    ReleaseDateUtc = DateTime.UtcNow,
                    ReplaceDateUtc = DateTime.UtcNow,
                    EncProducts = new List<EncProduct>
                    {
                        new EncProduct
                        {
                            Id = i,
                            Name = "ENC-Product",
                            Description = $"Demo enc cell list here to be added{i}",
                            IsForSale = true,
                            IssuerId = 1,
                            CountryId = 2,
                            EncProductTypeId = 1,
                            PriceBand = 1,
                            Type = "ENCPRODUCT",
                            ValidLicensePeriod = "3_Months"
                        }
                    }
                });
            }

            // Insert customers to the cache            
            //var redisDictionary = new RedisDictionary<string, List<EncCell>>("PALASH_ENC_CELL");
            //redisDictionary.Add("PALASH_ENC_CELL", encCells);

            //cache.KeyDelete("PALASH_ENC_CELL");
            //cache.StringSet("Dev_EncCell_UKHO", Compressor.Compress(JsonConvert.SerializeObject(encCells)));

            //var ss = cache.StringGet("PALASH_ENC_CELL");

            var ss = cache.StringGet("Dev_EncCells_UKHO");

            var encCellsJson = JsonConvert.DeserializeObject<List<EncCell>>(Compressor.Decompress(cache.StringGet("Dev_EncCells_UKHO")));

            for (int i = 0; i < 5; i++)
            {
                //var ts = TimeSpan.FromMinutes(20);
                cache.StringSet("PALASH_ENC_CELL", JsonConvert.SerializeObject(encCells[i]));
            }

        }
    }

    public class EncCell
    {
        public EncCell()
        {
            EncProducts = new List<EncProduct>();
        }

        public int Id { get; set; }

        public int IssuerId { get; set; }

        public string CellName { get; set; }

        public string Description { get; set; }

        public byte NavigationalPurpose { get; set; }

        public decimal BoundarySLat { get; set; }

        public decimal BoundaryWLon { get; set; }

        public decimal BoundaryNLat { get; set; }

        public decimal BoundaryELon { get; set; }

        public string PolygonPoints { get; set; }

        public short BaseEdition { get; set; }

        public DateTime BaseIssueDate { get; set; }

        public short? LatestUpdateNumber { get; set; }

        public DateTime? RecordLastUpdated { get; set; }

        public DateTime? AddedDate { get; set; }

        public string GeometryData { get; set; }

        public DateTime? ReleaseDateUtc { get; set; }

        public DateTime? CancelDateUtc { get; set; }

        public DateTime? ReplaceDateUtc { get; set; }

        public string Geography { get; set; }

        public int? DownloadedBaseEdition { get; set; }

        public int? DownloadedUpdateNumber { get; set; }

        public DateTime? DownloadedUtcDate { get; set; }

        public string Geometry { get; set; }

        public int? DownloadedIhoBaseEdition { get; set; }

        public int? DownloadedIhoUpdateNumber { get; set; }

        public DateTime? DownloadedIhoUtcDate { get; set; }

        public bool IsForSale { get; set; }

        public virtual Issuer Issuer { get; set; }

        public virtual List<EncProduct> EncProducts { get; set; }
    }

    public class Issuer
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string ShortName { get; set; }

        public bool Active { get; set; }
    }

    public class EncProduct
    {
        public int Id { get; set; }

        public string Type { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public string ValidLicensePeriod { get; set; }

        public bool IsForSale { get; set; }

        public int EncProductTypeId { get; set; }

        public int IssuerId { get; set; }

        public int PriceBand { get; set; }

        public int CountryId { get; set; }
    }

    public class RedisDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        private static ConnectionMultiplexer _cnn;
        private string _redisKey;

        public RedisDictionary(string redisKey)
        {
            _redisKey = redisKey;
            _cnn = ConnectionMultiplexer.Connect("prod-navi-sf.redis.cache.windows.net:6380,password=YX+fJAqjlNJGA1CF8pZoqI0mreBqVJcedRR4LbG3PHg=,ssl=True,abortConnect=False");
        }

        private IDatabase GetRedisDb()
        {
            return _cnn.GetDatabase();
        }
        private string Serialize(object obj)
        {
            return JsonConvert.SerializeObject(obj);
        }
        private T Deserialize<T>(string serialized)
        {
            return JsonConvert.DeserializeObject<T>(serialized);
        }
        public void Add(TKey key, TValue value)
        {
            GetRedisDb().HashSet(_redisKey, Serialize(key), Serialize(value));
        }
        public bool ContainsKey(TKey key)
        {
            return GetRedisDb().HashExists(_redisKey, Serialize(key));
        }
        public bool Remove(TKey key)
        {
            return GetRedisDb().HashDelete(_redisKey, Serialize(key));
        }
        public bool TryGetValue(TKey key, out TValue value)
        {
            var redisValue = GetRedisDb().HashGet(_redisKey, Serialize(key));
            if (redisValue.IsNull)
            {
                value = default(TValue);
                return false;
            }
            value = Deserialize<TValue>(redisValue.ToString());

            return true;
        }
        public ICollection<TValue> Values
        {
            get { return new Collection<TValue>(GetRedisDb().HashValues(_redisKey).Select(h => Deserialize<TValue>(h.ToString())).ToList()); }
        }
        public ICollection<TKey> Keys
        {
            get { return new Collection<TKey>(GetRedisDb().HashKeys(_redisKey).Select(h => Deserialize<TKey>(h.ToString())).ToList()); }
        }
        public TValue this[TKey key]
        {
            get
            {
                var redisValue = GetRedisDb().HashGet(_redisKey, Serialize(key));
                return redisValue.IsNull ? default(TValue) : Deserialize<TValue>(redisValue.ToString());
            }
            set
            {
                Add(key, value);
            }
        }
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }
        public void Clear()
        {
            GetRedisDb().KeyDelete(_redisKey);
        }
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return GetRedisDb().HashExists(_redisKey, Serialize(item.Key));
        }
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            GetRedisDb().HashGetAll(_redisKey).CopyTo(array, arrayIndex);
        }
        public int Count
        {
            get { return (int)GetRedisDb().HashLength(_redisKey); }
        }
        public bool IsReadOnly
        {
            get { return false; }
        }
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var db = GetRedisDb();
            foreach (var hashKey in db.HashKeys(_redisKey))
            {
                var redisValue = db.HashGet(_redisKey, hashKey);
                yield return new KeyValuePair<TKey, TValue>(Deserialize<TKey>(hashKey.ToString()), Deserialize<TValue>(redisValue.ToString()));
            }
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            yield return GetEnumerator();
        }
        public void AddMultiple(IEnumerable<KeyValuePair<TKey, TValue>> items)
        {
            GetRedisDb()
                .HashSet(_redisKey, items.Select(i => new HashEntry(Serialize(i.Key), Serialize(i.Value))).ToArray());
        }
    }
}
