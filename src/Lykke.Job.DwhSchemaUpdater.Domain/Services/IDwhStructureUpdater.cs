using System.Threading.Tasks;

namespace Lykke.Job.DwhSchemaUpdater.Domain.Services
{
    public interface IDwhStructureUpdater
    {
        Task UpdateDwhSchemaAsync();
    }
}
