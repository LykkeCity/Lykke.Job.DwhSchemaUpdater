using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.DwhSchemaUpdater.Settings.JobSettings
{
    public class DbSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [SqlCheck]
        public string SqlConnString { get; set; }
    }
}
