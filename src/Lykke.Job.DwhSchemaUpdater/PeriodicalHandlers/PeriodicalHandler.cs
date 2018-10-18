using System;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Common;
using Lykke.Common.Log;
using Lykke.Job.DwhSchemaUpdater.Domain.Services;

namespace Lykke.Job.DwhSchemaUpdater.PeriodicalHandlers
{
    public class PeriodicalHandler : IStartable, IStopable
    {
        private readonly TimerTrigger _timerTrigger;
        private readonly IDwhStructureUpdater _dwhStructureUpdater;

        public PeriodicalHandler(ILogFactory logFactory, IDwhStructureUpdater dwhStructureUpdater)
        {
            _timerTrigger = new TimerTrigger(nameof(PeriodicalHandler), TimeSpan.FromHours(24), logFactory);
            _timerTrigger.Triggered += Execute;

            _dwhStructureUpdater = dwhStructureUpdater;
        }

        public void Start()
        {
            _timerTrigger.Start();
        }
        
        public void Stop()
        {
            _timerTrigger.Stop();
        }

        public void Dispose()
        {
            _timerTrigger.Stop();
            _timerTrigger.Dispose();
        }

        private async Task Execute(ITimerTrigger timer, TimerTriggeredHandlerArgs args, CancellationToken cancellationToken)
        {
            await _dwhStructureUpdater.UpdateDwhSchemaAsync();
        }
    }
}
