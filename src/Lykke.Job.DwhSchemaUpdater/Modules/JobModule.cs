using Autofac;
using Common;
using Lykke.Job.DwhSchemaUpdater.Domain.Services;
using Lykke.Job.DwhSchemaUpdater.DomainServices;
using Lykke.Job.DwhSchemaUpdater.Services;
using Lykke.Job.DwhSchemaUpdater.Settings.JobSettings;
using Lykke.Sdk;
using Lykke.Sdk.Health;
using Lykke.Job.DwhSchemaUpdater.PeriodicalHandlers;

namespace Lykke.Job.DwhSchemaUpdater.Modules
{
    public class JobModule : Module
    {
        private readonly DwhSchemaUpdaterJobSettings _settings;

        public JobModule(DwhSchemaUpdaterJobSettings settings)
        {
            _settings = settings;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .AutoActivate()
                .SingleInstance();

            builder.RegisterType<DwhStructureUpdater>()
                .As<IDwhStructureUpdater>()
                .SingleInstance()
                .WithParameter("sqlConnString", _settings.Db.SqlConnString)
                .WithParameter("accountName", _settings.DwhBlobAccountName)
                .WithParameter("accountKey", _settings.DwhBlobAccountKey)
                .WithParameter("forcedUpdate", _settings.ForcedUpdate ?? false);

            builder.RegisterType<PeriodicalHandler>()
                .As<IStartable>()
                .As<IStopable>()
                .SingleInstance();
        }
    }
}
