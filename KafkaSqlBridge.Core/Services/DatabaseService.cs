using KafkaSqlBridge.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSqlBridge.Core.Services
{
    class DatabaseService
    {
        private readonly string _connectionString;

        public DatabaseService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task ProcessProductMessageAsync(ProductMessage message)
        {

        }
    }
}
