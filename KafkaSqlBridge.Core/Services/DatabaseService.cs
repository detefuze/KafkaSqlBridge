using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Security;
using Npgsql;
using Dapper;

namespace KafkaSqlBridge.Core.Services
{
    public class DatabaseService : IDatabaseService
    {
        private readonly string _connectionString;

        public DatabaseService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task ProcessProductMessageAsync(ProductMessage message)
        {
            const string sql = @"
                INSERT INTO products (
                    product_code, 
                    product_name, 
                    pcs_ca, 
                    ca_massa_n, 
                    ca_massa_b, 
                    wip_code
                ) VALUES (
                    @product_code, 
                    @product_name, 
                    @pcs_ca, 
                    @ca_massa_n, 
                    @ca_massa_b, 
                    @wip_code
                )
                ON CONFLICT (product_code) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    pcs_ca = EXCLUDED.pcs_ca,
                    ca_massa_n = EXCLUDED.ca_massa_n,
                    ca_massa_b = EXCLUDED.ca_massa_b,
                    wip_code = EXCLUDED.wip_code";

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(sql, message);

        }

        public async Task ProcessMaterialMessageAsync(ProductMessage message)
        {
             
        }
    }
}
