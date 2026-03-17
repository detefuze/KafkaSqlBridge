using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Security;
using Microsoft.Data.SqlClient;
using Dapper;
using System.ComponentModel;

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

        public async Task ProcessMaterialMessageAsync(MaterialMessage message)
        {
            const string sql = @"
            MERGE INTO pms_materials AS target
            USING (SELECT @material_code AS material_code) AS source
            ON target.material_code = source.material_code
            WHEN MATCHED THEN
                UPDATE SET 
                    material_name = @material_name,
                    material_type = @material_type
            WHEN NOT MATCHED THEN
                INSERT (material_code, material_name, material_type)
                VALUES (@material_code, @material_name, @material_type);";

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(sql, message);
        }
    }
}
