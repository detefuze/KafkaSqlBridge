using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Security;
using Microsoft.Data.SqlClient;
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
            MERGE INTO products AS target
            USING (SELECT @product_code AS product_code) AS source
            ON target.product_code = source.product_code
            WHEN MATCHED THEN
                UPDATE SET 
                    product_name = @product_name,
                    PCS_CA = @pcs_ca,
                    CA_massa_n = @ca_massa_n,
                    CA_massa_b = @ca_massa_b,
                    wip_code = @wip_code
            WHEN NOT MATCHED THEN
                INSERT (product_code, product_name, PCS_CA, CA_massa_n, CA_massa_b, wip_code)
                VALUES (@product_code, @product_name, @pcs_ca, @ca_massa_n, @ca_massa_b, @wip_code);";

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(sql, message);

        }

        public async Task ProcessMaterialMessageAsync(ProductMessage message)
        {
             
        }
    }
}
