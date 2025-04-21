from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition

# Partition par année (mettre à jour jusqu'à 2025)
years = [str(year) for year in range(2000, 2026)]
yearly_partitions = StaticPartitionsDefinition(years)

# Partition par surface
surfaces = ["Hard", "Clay", "Grass", "Carpet"]
surface_partitions = StaticPartitionsDefinition(surfaces)

# Partition combinée année + surface
year_surface_partitions = MultiPartitionsDefinition(
    {
        "year": yearly_partitions,
        "surface": surface_partitions
    }
)