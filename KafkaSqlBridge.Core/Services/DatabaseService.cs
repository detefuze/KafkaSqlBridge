using Dapper;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Net.NetworkInformation;
using System.Security;
using System.Text;
using System.Text.Json;
using System.Transactions;

namespace KafkaSqlBridge.Core.Services
{
    public class DatabaseService : IDatabaseService
    {
        private readonly ILogger<DatabaseService> _logger;
        private readonly string _connectionString;

        public DatabaseService(string connectionString, ILogger<DatabaseService> logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task ProcessProductBatchAsync(List<ProductMessage> batch)
        {
            if (batch  == null || batch.Count == 0) { return; }

            var table = new DataTable();

            table.Columns.Add("product_code", typeof(string));
            table.Columns.Add("product_name", typeof(string));
            table.Columns.Add("PCS_CA", typeof(int));
            table.Columns.Add("CA_massa_n", typeof(decimal));
            table.Columns.Add("CA_massa_b", typeof(decimal));
            table.Columns.Add("wip_code", typeof(string));

            foreach (var item in batch)
            {
                table.Rows.Add(
                    item.product_code,
                    item.product_name,
                    item.PCS_CA,
                    item.CA_massa_n,
                    item.CA_massa_b,
                    item.wip_code
                    );
            }

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync("kafka_import_products_batch", 
                new {products = table},
                commandType: CommandType.StoredProcedure);
        }

        public async Task ProcessMaterialBatchAsync(List<MaterialMessage> batch)
        {
            if (batch == null || batch.Count == 0) { return; }

            var table = new DataTable();

            table.Columns.Add("material_code", typeof(string));
            table.Columns.Add("material_name", typeof(string));
            table.Columns.Add("material_type", typeof(int));

            foreach (var item in batch)
            {
                table.Rows.Add(
                    item.material_code,
                    item.material_name,
                    item.material_type
                    );
            }

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync("kafka_import_material_batch",
                new { material = table },
                commandType: CommandType.StoredProcedure);
        }

        //public async Task ProcessProductMessageAsync(ProductMessage message)
        //{
        //    using var connection = new SqlConnection(_connectionString);
        //    await connection.OpenAsync();
        //    try
        //    {
        //        const string sql = @"
        //        EXEC kafka_import_products 
        //        @product_code = @product_code,
        //        @product_name = @product_name,
        //        @PCS_CA = @PCS_CA,
        //        @CA_massa_n = @CA_massa_n,
        //        @CA_massa_b = @CA_massa_b,
        //        @wip_code = @wip_code;";

        //        await connection.ExecuteAsync(sql, message);
        //        _logger.LogInformation("Запись {code} в БД прошла успешно", message.product_code);
        //    }
        //    catch (Exception)
        //    {
        //        _logger.LogInformation("Произошла ошибка при записи сообщения в БД");
        //        throw;
        //    }


        //}

        //public async Task ProcessMaterialMessageAsync(MaterialMessage message)
        //{
        //    using var connection = new SqlConnection(_connectionString);
        //    await connection.OpenAsync();

        //    try
        //    {
        //        const string sql = @"
        //        EXEC kafka_import_material 
        //        @material_code = @material_code,
        //        @material_name = @material_name,
        //        @material_type = @material_type;";

        //        await connection.ExecuteAsync(sql, message);
        //        //_logger.LogInformation("Запись {code} в БД прошла успешно", message.material_code);
        //    }
        //    catch (Exception)
        //    {
        //        _logger.LogInformation("Произошла ошибка при записи сообщения в БД");
        //        throw;
        //    }
        //}
    }
}
