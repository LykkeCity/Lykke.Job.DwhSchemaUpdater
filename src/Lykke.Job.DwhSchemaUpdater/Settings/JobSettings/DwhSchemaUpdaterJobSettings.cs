namespace Lykke.Job.DwhSchemaUpdater.Settings.JobSettings
{
    public class DwhSchemaUpdaterJobSettings
    {
        public DbSettings Db { get; set; }

        public string DwhBlobAccountName { get; set; }

        public string DwhBlobAccountKey { get; set; }
    }
}
