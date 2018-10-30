using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.DwhSchemaUpdater.Domain;
using Lykke.Job.DwhSchemaUpdater.Domain.Services;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Lykke.Job.DwhSchemaUpdater.DomainServices
{
    public class DwhStructureUpdater : IDwhStructureUpdater
    {
        private const string _structureFileName = "TableStructure.str2";
        private const string _structureUpdateFileName = "lastStructureUpdate.txt";
        private const int _maxRetryCount = 5;

        private readonly string _sqlConnString;
        private readonly string _accountName;
        private readonly string _accountKey;
        private readonly ILog _log;
        private readonly CloudBlobClient _blobClient;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };

        private bool _forcedUpdate;

        public DwhStructureUpdater(
            ILogFactory logFactory,
            string sqlConnString,
            string accountName,
            string accountKey,
            bool forcedUpdate = false)
        {
            _log = logFactory.CreateLog(this);
            _sqlConnString = sqlConnString;
            _accountName = accountName;
            _accountKey = accountKey;
            var account = new CloudStorageAccount(new StorageCredentials(_accountName, _accountKey), true);
            _blobClient = account.CreateCloudBlobClient();

            _forcedUpdate = forcedUpdate;
        }

        public async Task UpdateDwhSchemaAsync()
        {
            BlobContinuationToken token = null;
            while (true)
            {
                ContainerResultSegment containersResult = await _blobClient.ListContainersSegmentedAsync(token);
                if (containersResult?.Results != null)
                    foreach (var container in containersResult.Results)
                    {
                        await ProcessContainerAsync(container);
                    }

                token = containersResult?.ContinuationToken;
                if (token == null)
                    break;
            }

            if (_forcedUpdate)
                _forcedUpdate = false;

            _log.Info("Dwh structure update is finished");
        }

        private async Task ProcessContainerAsync(CloudBlobContainer container)
        {
            Console.WriteLine($"Processing container - {container.Name}");

            var tablesStructure = await GetStructureFromContainerAsync(container);
            if (tablesStructure == null)
                return;

            bool updateRequired = await CheckUpdateRequiredAsync(container);
            if (!_forcedUpdate && !updateRequired)
                return;

            var columnsListDict = GetColumnsListsFromStructure(tablesStructure);
            foreach (var tableStructure in tablesStructure.Tables)
            {
                Console.WriteLine($"\tSetting schema for table {tableStructure.TableName}");

                var sql = GenerateSqlCommand(
                    tableStructure.TableName,
                    container.Name,
                    tableStructure.AzureBlobFolder,
                    columnsListDict[tableStructure.TableName]);
                int retryCount = 0;
                while (true)
                {
                    try
                    {
                        using (SqlConnection connection = new SqlConnection(_sqlConnString))
                        {
                            await connection.OpenAsync();
                            SqlCommand command = new SqlCommand(sql, connection);
                            await command.ExecuteNonQueryAsync();
                        }
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        ++retryCount;
                        if (retryCount > _maxRetryCount)
                            throw;

                        await Task.Delay(TimeSpan.FromSeconds(retryCount));
                    }
                }
            }

            var updateBlob = container.GetBlockBlobReference(_structureUpdateFileName);
            await updateBlob.UploadTextAsync(string.Empty);
        }

        private async Task<bool> CheckUpdateRequiredAsync(CloudBlobContainer container)
        {
            var updateBlob = container.GetBlockBlobReference(_structureUpdateFileName);
            if (!await updateBlob.ExistsAsync())
                return true;

            var structureBlob = container.GetBlockBlobReference(_structureFileName);
            if (!await structureBlob.ExistsAsync())
                return false;

            var structureDate = structureBlob.Properties.LastModified ?? structureBlob.Properties.Created;
            var updateDate = updateBlob.Properties.LastModified ?? updateBlob.Properties.Created;
            return structureDate > updateDate;
        }

        private async Task<TablesStructure> GetStructureFromContainerAsync(CloudBlobContainer container)
        {
            var blob = container.GetBlockBlobReference(_structureFileName);
            if (!await blob.ExistsAsync())
                return null;

            string structureStr = await blob.DownloadTextAsync(null, _blobRequestOptions, null);
            return structureStr.DeserializeJson<TablesStructure>();
        }

        private string GenerateSqlCommand(
            string tableName,
            string containerName,
            string blobFolder,
            string columnsList)
        {
            return $"exec CreateOrRepalceExternalTablev2 @StorageAccountName='{_accountName}', @StorageAccountKey='{_accountKey}', @containername='{containerName}'"
                + $", @TableName='{tableName}', @AzureBlobFolder='{blobFolder}', @ColumnList='{columnsList}', @FileFormat=NULL";
        }

        private Dictionary<string, string> GetColumnsListsFromStructure(TablesStructure tablesStructure)
        {
            var result = new Dictionary<string, string>();

            foreach (var tableStructure in tablesStructure.Tables)
            {
                var strBuilder = new StringBuilder();
                var columns = tableStructure.Columns ?? tableStructure.Colums;
                foreach (var columnInfo in columns)
                {
                    if (strBuilder.Length > 0)
                        strBuilder.Append(", ");
                    strBuilder.Append($"[{columnInfo.ColumnName}] ");
                    if (columnInfo.ColumnType == typeof(DateTime).Name)
                        strBuilder.Append("DATETIME");
                    else if (columnInfo.ColumnType == typeof(double).Name ||
                             columnInfo.ColumnType == typeof(decimal).Name)
                        strBuilder.Append("Decimal(23,8)");
                    else if (columnInfo.ColumnType == typeof(bool).Name)
                        strBuilder.Append("Bit");
                    else
                        strBuilder.Append("VARCHAR(256)");
                }

                result[tableStructure.TableName] = strBuilder.ToString();
            }

            return result;
        }
    }
}
