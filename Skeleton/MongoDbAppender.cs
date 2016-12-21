using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using log4net.Appender;
using log4net.Core;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoAppender.Skeleton
{
    public class MongoDbAppender : AppenderSkeleton
    {
        private readonly object _lock = new object();
        private volatile IMongoCollection<BsonDocument> _collection;
        private readonly List<MongoAppenderFileld> _fields = new List<MongoAppenderFileld>();

        public string ConnectionString { get; set; }

        public string ConnectionStringName { get; set; }

        public string CollectionName { get; set; }

        public string DatabaseName { get; set; }

        public string CertificateFriendlyName { get; set; }

        public long ExpireAfterSeconds { get; set; }

        public string NewCollectionMaxDocs { get; set; }

        public string NewCollectionMaxSize { get; set; }

        public void AddField(MongoAppenderFileld fileld)
        {
            _fields.Add(fileld);
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            var collection = GetCollection();
            collection.InsertOneAsync(BuildBsonDocument(loggingEvent));
            CreateExpiryAfterIndex(collection);
        }

        protected override void Append(LoggingEvent[] loggingEvents)
        {
            var collection = GetCollection();
            collection.InsertManyAsync(loggingEvents.Select(BuildBsonDocument));
            CreateExpiryAfterIndex(collection);
        }

        private IMongoCollection<BsonDocument> GetCollection()
        {
            if (_collection != null) return _collection;

            lock (_lock)
            {
                if (_collection != null) return _collection;

                var db = GetDatabase();
                var collectionName = CollectionName ?? "Logs";

                EnsureCollectionExists(db, collectionName);

                _collection = db.GetCollection<BsonDocument>(collectionName);
                return _collection;
            }
        }

        private void EnsureCollectionExists(IMongoDatabase db, string collectionName)
        {
            if (!CollectionExists(db, collectionName))
            {
                CreateCollection(db, collectionName);
            }
        }

        private bool CollectionExists(IMongoDatabase db, string collectionName)
        {
            var filter = new BsonDocument("name", collectionName);

            return db.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter })
                     .Result
                     .ToListAsync()
                     .Result
                     .Any();
        }

        private void CreateCollection(IMongoDatabase db, string collectionName)
        {
            var cob = new CreateCollectionOptions();

            SetCappedCollectionOptions(cob);

            db.CreateCollectionAsync(collectionName, cob).GetAwaiter().GetResult();
        }

        private void SetCappedCollectionOptions(CreateCollectionOptions options)
        {
            var unitResolver = new MongoUnitResolver();

            var newCollectionMaxSize = unitResolver.Resolve(NewCollectionMaxSize);
            var newCollectionMaxDocs = unitResolver.Resolve(NewCollectionMaxDocs);

            if (newCollectionMaxSize > 0)
            {
                options.Capped = true;
                options.MaxSize = newCollectionMaxSize;

                if (newCollectionMaxDocs > 0)
                {
                    options.MaxDocuments = newCollectionMaxDocs;
                }
            }
        }

        private string GetConnectionString()
        {
            var connectionStringSetting = ConfigurationManager.ConnectionStrings[ConnectionStringName];
            return connectionStringSetting != null ? connectionStringSetting.ConnectionString : ConnectionString;
        }

        private IMongoDatabase GetDatabase()
        {
            string connStr = GetConnectionString();

            if (string.IsNullOrWhiteSpace(connStr))
            {
                throw new InvalidOperationException("Must provide a valid connection string");
            }

            var url = MongoUrl.Create(connStr);
            var settings = MongoClientSettings.FromUrl(url);
            settings.SslSettings = url.UseSsl ? GetSslSettings() : null;
            var client = new MongoClient(settings);

            var db = client.GetDatabase(url.DatabaseName ?? "Logging");
            return db;
        }

        private SslSettings GetSslSettings()
        {
            SslSettings sslSettings = null;

            if (!string.IsNullOrEmpty(CertificateFriendlyName))
            {
                X509Certificate2 certificate = GetCertificate(CertificateFriendlyName);

                if (null != certificate)
                {
                    sslSettings = new SslSettings();
                    sslSettings.ClientCertificates = new List<X509Certificate2>() { certificate };
                }
            }

            return sslSettings;
        }

        private X509Certificate2 GetCertificate(string certificateFriendlyName)
        {
            X509Certificate2 certificateToReturn = null;
            var store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
            store.Open(OpenFlags.ReadOnly);

            var certificates = store.Certificates;

            foreach (X509Certificate2 certificate in certificates)
            {
                if (certificate.FriendlyName.Equals(certificateFriendlyName))
                {
                    certificateToReturn = certificate;
                    break;
                }
            }

            store.Close();

            return certificateToReturn;
        }

        private BsonDocument BuildBsonDocument(LoggingEvent log)
        {
            var doc = new BsonDocument();
            foreach (MongoAppenderFileld field in _fields)
            {
                object value = field.Layout.Format(log);
                var bsonValue = value as BsonValue ?? BsonValue.Create(value);
                doc.Add(field.Name, bsonValue);
            }
            return doc;
        }

        private void CreateExpiryAfterIndex(IMongoCollection<BsonDocument> collection)
        {
            if (ExpireAfterSeconds <= 0) return;
            collection.Indexes.CreateOneAsync(
                Builders<BsonDocument>.IndexKeys.Ascending("timestamp"),
                new CreateIndexOptions()
                {
                    Name = "expireAfterSecondsIndex",
                    ExpireAfter = new TimeSpan(ExpireAfterSeconds * TimeSpan.TicksPerSecond)
                });
        }
    }
}