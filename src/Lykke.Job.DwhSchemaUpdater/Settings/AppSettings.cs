using Lykke.Job.DwhSchemaUpdater.Settings.JobSettings;
using Lykke.Sdk.Settings;

namespace Lykke.Job.DwhSchemaUpdater.Settings
{
    public class AppSettings : BaseAppSettings
    {
        public DwhSchemaUpdaterJobSettings DwhSchemaUpdaterJob { get; set; }
    }
}
